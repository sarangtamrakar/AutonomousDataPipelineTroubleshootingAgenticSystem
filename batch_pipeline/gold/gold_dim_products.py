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
        .appName("Medallion_Gold_DimProduct_Processing")
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
silver_products = spark.read.format("delta").load(f"{s3_silver_base}/products")

print("Starting Product dim Gold Pipeline...")


# ==========================================
# 3. DIMENSION TABLE: Products (SCD Type 2 History)
# ==========================================
print("Processing dim_product...")
dim_product_updates = (
    silver_products.select("product_id", "product_name", "category", "unit_price")
    .withColumn("start_date", current_timestamp())
    .withColumn("end_date", lit(None).cast("timestamp"))
    .withColumn("current_flag", lit(True))
    .withColumn("_gold_updated_at", current_timestamp())
)

gold_product_path = f"{s3_gold_base}/dim_products"

if DeltaTable.isDeltaTable(spark, gold_product_path):
    gold_product_table = DeltaTable.forPath(spark, gold_product_path)

    # 1) Expire current rows when business attributes change
    gold_product_table.alias("target").merge(
        dim_product_updates.alias("source"),
        "target.product_id = source.product_id AND target.current_flag = true"
    ).whenMatchedUpdate(
        condition=(
            "target.product_name IS DISTINCT FROM source.product_name OR "
            "target.category IS DISTINCT FROM source.category OR "
            "target.unit_price IS DISTINCT FROM source.unit_price"
        ),
        set={
            "current_flag": lit(False),
            "end_date": current_timestamp()
        }
    ).execute()

    # 2) Insert new or changed current rows
    gold_product_table.alias("target").merge(
        dim_product_updates.alias("source"),
        "target.product_id = source.product_id AND target.current_flag = true"
    ).whenNotMatchedInsertAll().execute()
else:
    dim_product_updates.write.format("delta").mode("overwrite").save(gold_product_path)

spark.stop()
print("Product Gold pipeline execution complete!")