from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import os

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


def get_session():
    spark = (
        SparkSession.builder
        .master("spark://spark-master:7077")
        .appName("Medallion_Silver_Products_Processing")
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
        .config("spark.sql.warehouse.dir", "s3a://sarang-de/warehouse")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.driver.extraClassPath", "/opt/spark/jars/*")
        .config("spark.executor.extraClassPath", "/opt/spark/jars/*")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "s3a://sarang-de/spark-event-logs")
        .getOrCreate()
    )

    return spark


s3_bronze_base = "s3a://sarang-de/bronze"
s3_silver_base = "s3a://sarang-de/silver"


def main():
    spark = get_session()

    print("Processing Products...")
    products_bronze = spark.read.format("delta").load(f"{s3_bronze_base}/products")

    products_silver = products_bronze \
        .dropDuplicates(["product_id"]) \
        .dropna(subset=["product_id"]) \
        .withColumn("_silver_processed_at", current_timestamp())

    products_silver.write.format("delta") \
        .mode("overwrite") \
        .save(f"{s3_silver_base}/products")

    print(f"Cleaned Products saved to {s3_silver_base}/products")
    spark.stop()


if __name__ == "__main__":
    main()
