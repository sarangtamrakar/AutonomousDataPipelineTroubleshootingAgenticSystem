import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Custom callbacks for SRE Agent triggers and PostgreSQL Lineage tracking
from callbacks import task_success_callback, task_failure_callback, dag_failure_callback

# Path to the scripts inside the airflow/spark containers
HOST_BATCH_PIPELINE_DIR=os.environ.get("HOST_BATCH_PIPELINE_DIR","/Users/sarang/sarangde/project/AutonomousDataPipelineTroubleshootingAgenticSystem/batch_pipeline")
DOCKER_BATCH_PIPELINE_DIR=os.environ.get("DOCKER_BATCH_PIPELINE_DIR" , "/opt/airflow/batch_pipeline")


# Path to docker-compose file of spark-cli (from the perspective of the Airflow worker)
SPARK_SETUP_HOST_DIR=os.environ.get("SPARK_SETUP_HOST_DIR","/Users/sarang/sarangde/docker_infra/infra/spark-delta-s3-setup")
SPARK_SETUP_DOCKER_DIR=os.environ.get("SPARK_SETUP_DOCKER_DIR","/opt/airflow/spark-delta-s3-setup")



default_args = {
    "owner": "data_engineering",
    "on_success_callback": task_success_callback,
    "on_failure_callback": task_failure_callback,
    "retries": 0  # 0 retries ensures the RCA agent investigates the exact first failure
}

# def build_spark_command(script_path):
#     """
#     Triggers the dormant 'spark-submit-cli' service defined in docker-compose.yml.
#     Since SparkSession.builder in the Python scripts handles the master URL, 
#     Delta configs, and AWS keys, we only need to pass the script path!
#     """
#     return f"""
#         docker compose -f {COMPOSE_FILE_DOCKER_PATH} run --rm spark-submit-cli \\
#         /opt/spark/bin/spark-submit \\
#         {DOCKER_BATCH_PIPELINE_DIR}/{script_path}
#     """


def build_spark_command(script_path):
    # Retrieve AWS credentials from Airflow's environment
    aws_key = os.environ.get('AWS_ACCESS_KEY_ID', 'MINIO') 
    aws_secret = os.environ.get('AWS_SECRET_ACCESS_KEY', 'MINIO')
    
    
    
    return f"""
        docker run --rm \\
        --network spark-net \\
        -v {HOST_BATCH_PIPELINE_DIR}:{DOCKER_BATCH_PIPELINE_DIR} \\
        -v {SPARK_SETUP_HOST_DIR}/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \\
        -v {SPARK_SETUP_HOST_DIR}/jars/aws-java-sdk-bundle-1.12.262.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \\
        -v {SPARK_SETUP_HOST_DIR}/jars/delta-spark_2.12-3.0.0.jar:/opt/spark/jars/delta-spark_2.12-3.0.0.jar \\
        -v {SPARK_SETUP_HOST_DIR}/jars/delta-storage-3.0.0.jar:/opt/spark/jars/delta-storage-3.0.0.jar \\
        -v {SPARK_SETUP_HOST_DIR}/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/hadoop-aws-3.3.4.jar \\
        -v {SPARK_SETUP_HOST_DIR}/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar:/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \\
        -v {SPARK_SETUP_HOST_DIR}/jars/kafka-clients-3.4.1.jar:/opt/spark/jars/kafka-clients-3.4.1.jar \\
        -v {SPARK_SETUP_HOST_DIR}/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar:/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \\
        -v {SPARK_SETUP_HOST_DIR}/jars/commons-pool2-2.11.1.jar:/opt/spark/jars/commons-pool2-2.11.1.jar \\
        -e AWS_ACCESS_KEY_ID={aws_key} \\
        -e AWS_SECRET_ACCESS_KEY={aws_secret} \\
        apache/spark:3.5.1 \\
        /opt/spark/bin/spark-submit \\
        /opt/airflow/batch_pipeline/{script_path}
    """



with DAG(
    dag_id="master_medallion_pipeline_flat",
    default_args=default_args,
    start_date=datetime(2026, 6, 16),
    schedule_interval="@daily",
    catchup=False,
    on_failure_callback=dag_failure_callback 
) as dag:

    # =======================================================
    # 1. BRONZE LAYER (Extract & Load)
    # =======================================================
    run_bronze_customers = BashOperator(
        task_id='run_bronze_customers',
        params={"target_asset_id": "urn:bronze:customers"},
        bash_command=build_spark_command("bronze/bronze_customers.py")
    )

    run_bronze_products = BashOperator(
        task_id='run_bronze_products',
        params={"target_asset_id": "urn:bronze:products"},
        bash_command=build_spark_command("bronze/bronze_products.py")
    )

    run_bronze_sales = BashOperator(
        task_id='run_bronze_sales',
        params={"target_asset_id": "urn:bronze:sales"},
        bash_command=build_spark_command("bronze/bronze_sales.py")
    )

    # =======================================================
    # 2. SILVER LAYER (Cleanse & Standardize)
    # =======================================================
    run_silver_customers = BashOperator(
        task_id='run_silver_customers',
        params={"target_asset_id": "urn:silver:customers"},
        bash_command=build_spark_command("silver/silver_customers.py")
    )

    run_silver_products = BashOperator(
        task_id='run_silver_products',
        params={"target_asset_id": "urn:silver:products"},
        bash_command=build_spark_command("silver/silver_products.py")
    )

    run_silver_sales = BashOperator(
        task_id='run_silver_sales',
        params={"target_asset_id": "urn:silver:sales"},
        bash_command=build_spark_command("silver/silver_sales.py")
    )

    # =======================================================
    # 3. GOLD LAYER (Business Value & Star Schema)
    # =======================================================
    run_gold_dim_customers = BashOperator(
        task_id='build_dim_customers',
        params={"target_asset_id": "urn:gold:dim_customers"},
        bash_command=build_spark_command("gold/gold_dim_customers.py")
    )

    run_gold_dim_products = BashOperator(
        task_id='build_dim_products',
        params={"target_asset_id": "urn:gold:dim_products"},
        bash_command=build_spark_command("gold/gold_dim_products.py")
    )

    run_gold_fact_sales = BashOperator(
        task_id='build_fact_sales',
        params={"target_asset_id": "urn:gold:fact_sales"},
        bash_command=build_spark_command("gold/gold_fact_sales.py")
    )

    # =======================================================
    # ORCHESTRATION DEPENDENCIES
    # =======================================================
    
    # Track 1: Customers (Vertical Pipeline)
    run_bronze_customers >> run_silver_customers >> run_gold_dim_customers
    
    # Track 2: Products (Vertical Pipeline)
    run_bronze_products >> run_silver_products >> run_gold_dim_products
    
    # Track 3: Sales (Vertical Pipeline up to Silver)
    run_bronze_sales >> run_silver_sales
    
    # CROSS-DOMAIN DEPENDENCY: 
    # The Fact table MUST wait for its own Silver Sales data AND the Gold Dimensions 
    # so it can look up the correct Surrogate Keys.
    [
        run_silver_sales, 
        run_gold_dim_customers, 
        run_gold_dim_products
    ] >> run_gold_fact_sales