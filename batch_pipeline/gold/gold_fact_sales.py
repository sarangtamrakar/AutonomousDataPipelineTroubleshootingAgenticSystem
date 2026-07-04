from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, current_timestamp
from delta.tables import DeltaTable
import os
# 1. Initialize Spark Session

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

def get_session():
    spark = (
        SparkSession.builder
        .master("spark://spark-master:7077")
        .appName("Medallion_Gold_SalesFact_Processing")
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

# Read Silver and Gold Data
silver_sales = spark.read.format("delta").load(f"{s3_silver_base}/sales")

gold_products = spark.read.format("delta").load(f"{s3_gold_base}/dim_products")
current_gold_products = gold_products.filter(col("current_flag") == True)

print("Starting Sales Fact Gold Pipeline...")

# ==========================================
# 3. FACT TABLE: Sales (Idempotent Merge)
# ==========================================
print("Processing fact_sales...")

# First, enrich the sales data with product prices from the gold product dimension
fact_sales_updates = silver_sales.alias("s") \
    .join(current_gold_products.alias("p"), col("s.product_id") == col("p.product_id"), "left") \
    .select(
        col("s.sale_id"),
        col("s.customer_id"),
        col("s.product_id"),
        col("s.sale_date"),
        col("s.quantity_sold"),
        round(col("s.quantity_sold") * col("p.unit_price"), 2).alias("total_sale_amount")
    ).withColumn("_gold_updated_at", current_timestamp())

gold_sales_path = f"{s3_gold_base}/fact_sales"

if DeltaTable.isDeltaTable(spark, gold_sales_path):
    gold_sales_table = DeltaTable.forPath(spark, gold_sales_path)
    
    # PRODUCTION STANDARD: Idempotent Merge on Primary Key
    # Even though fact tables are usually "append only", merging on the primary key (sale_id)
    # guarantees that if this pipeline runs twice by accident, it won't duplicate sales!
    gold_sales_table.alias("target").merge(
        fact_sales_updates.alias("source"),
        "target.sale_id = source.sale_id"
    ).whenMatchedUpdateAll(
        # Optional: Updates the record if the sale status/amount changed
    ).whenNotMatchedInsertAll().execute()
else:
    # We partition large fact tables by date to optimize Athena queries!
    fact_sales_updates.write.format("delta") \
        .partitionBy("sale_date") \
        .mode("overwrite") \
        .save(gold_sales_path)

spark.stop()
print("Production Gold Sales Fact pipeline execution complete!")