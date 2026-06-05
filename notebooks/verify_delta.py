from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("VerifyDelta") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "bigdata123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("📖 Reading Delta table from s3a://ecommerce-lakehouse/events_raw/...")
    try:
        df = spark.read.format("delta").load("s3a://ecommerce-lakehouse/events_raw/")
        print("✅ Successfully loaded Delta table. Printing top 5 rows:")
        df.show(5, truncate=False)
    except Exception as e:
        print(f"❌ Failed to read delta table: {e}")

if __name__ == "__main__":
    main()
