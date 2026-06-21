from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("ValidateFeatures") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "bigdata123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    df = spark.read.format("delta").load("s3a://ecommerce-lakehouse/features_prepared/")
    n = df.count()
    print(f"features_prepared row count: {n}")
    df.groupBy("label").count().show()
    assert n > 0, "Validation FAILED: features_prepared is empty"
    print("Validation PASSED")


if __name__ == "__main__":
    main()
