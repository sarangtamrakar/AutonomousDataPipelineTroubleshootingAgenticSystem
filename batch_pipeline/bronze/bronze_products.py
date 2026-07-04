from pyspark.sql import SparkSession
import os

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


def get_session():
    spark = (
        SparkSession.builder
        .master("spark://spark-master:7077")
        .appName("Medallion_Bronze_Products_Processing")
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


s3_raw_base_path = "s3a://sarang-de/raw"
bronze_base_path = "s3a://sarang-de/bronze"


def main():
    spark = get_session()
    table_name = "products"
    raw_input_path = f"{s3_raw_base_path}/{table_name}/{table_name}.csv"
    bronze_output_path = f"{bronze_base_path}/{table_name}"

    print("Starting Bronze ingestion for products...")

    raw_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(raw_input_path)

    raw_df.write.format("delta") \
        .mode("append") \
        .save(bronze_output_path)

    spark.stop()
    print(f"Successfully ingested products into Bronze layer at {bronze_output_path}")


if __name__ == "__main__":
    main()
