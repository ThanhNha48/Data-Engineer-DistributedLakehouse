from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Validate Bronze Seeds")
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

tables = spark.sql("SHOW TABLES IN iceberg.bronze").collect()

total = 0
for row in tables:
    table = f"iceberg.bronze.{row.tableName}"
    count = spark.sql(f"SELECT count(*) FROM {table}").collect()[0][0]
    total += count
    print(f"{table}: {count}")

print(f"Tổng cộng: {total} bản ghi")
spark.stop()
