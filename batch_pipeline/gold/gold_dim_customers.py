from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, current_timestamp, lit
from delta.tables import DeltaTable
import os

# 1. Initialize Spark Session
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

def get_session():
    spark = (
        SparkSession.builder
        .master("spark://spark-master:7077")
        .appName("Medallion_Gold_DimCustomer_Processing")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        .config("spark.sql.warehouse.dir" , "s3a://sarang-de/warehouse")
        .config("spark.hadoop.fs.s3a.path.style.access" , "true")
        .config("spark.hadoop.fs.s3a.access.key",AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key",AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.endpoint","s3.amazonaws.com")
        .config("spark.driver.extraClassPath", "/opt/spark/jars/*")
        .config("spark.executor.extraClassPath", "/opt/spark/jars/*")
        .config("spark.eventLog.enabled" ,"true")
        .config("spark.eventLog.dir","s3a://sarang-de/spark-event-logs")
        .getOrCreate()
    )

    return spark


spark = get_session()



# 2. Define your S3 paths
s3_silver_base = "s3a://sarang-de/silver"
s3_gold_base = "s3a://sarang-de/gold"

# Read Silver Data
silver_customers = spark.read.format("delta").load(f"{s3_silver_base}/customers")

print("Starting customer dim Gold Pipeline...")

# ==========================================
# 3. DIMENSION TABLE: Customers (SCD Type 2 History)
# ==========================================
print("Processing dim_customer...")
dim_customer_updates = (
    silver_customers.select("customer_id", "customer_name", "region")
    .withColumn("start_date", current_timestamp())
    .withColumn("end_date", lit(None).cast("timestamp"))
    .withColumn("current_flag", lit(True))
    .withColumn("_gold_updated_at", current_timestamp())
)

gold_customer_path = f"{s3_gold_base}/dim_customers"

# Check if the Delta table already exists in S3
if DeltaTable.isDeltaTable(spark, gold_customer_path):
    gold_customer_table = DeltaTable.forPath(spark, gold_customer_path)

    # 1) Expire current rows when business attributes change
    gold_customer_table.alias("target").merge(
        dim_customer_updates.alias("source"),
        "target.customer_id = source.customer_id AND target.current_flag = true"
    ).whenMatchedUpdate(
        condition=(
            "target.customer_name IS DISTINCT FROM source.customer_name OR "
            "target.region IS DISTINCT FROM source.region"
        ),
        set={
            "current_flag": lit(False),
            "end_date": current_timestamp()
        }
    ).execute()

    # 2) Insert new or changed current rows
    gold_customer_table.alias("target").merge(
        dim_customer_updates.alias("source"),
        "target.customer_id = source.customer_id AND target.current_flag = true"
    ).whenNotMatchedInsertAll().execute()
else:
    # First time run: Create the table with SCD2 metadata
    dim_customer_updates.write.format("delta").mode("overwrite").save(gold_customer_path)


spark.stop()
print("Customer Gold pipeline execution completed!")