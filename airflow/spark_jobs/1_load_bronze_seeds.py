import os
from pathlib import Path
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Load Bronze Seeds")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
    .config("spark.sql.catalog.iceberg.uri", "http://nessie:19120/api/v1")
    .config("spark.sql.catalog.iceberg.ref", "main")
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "admin123")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .getOrCreate()
)

seeds_dir = "/opt/airflow/ecommerce_dbt/seeds"

for csv_file in os.listdir(seeds_dir):
    if not csv_file.endswith(".csv"):
        continue

    table_name = Path(csv_file).stem
    full_table = f"iceberg.bronze.{table_name}"

    df = spark.read.csv(
        f"{seeds_dir}/{csv_file}",
        header=True,
        inferSchema=True,
    )

    (
        df.writeTo(full_table)
        .tableProperty("format-version", "2")
        .createOrReplace()
    )

    print(f"✅ Loaded {df.count()} records into {full_table}")

spark.stop()
