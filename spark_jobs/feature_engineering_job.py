from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, max as _max, min as _min, when, unix_timestamp

def main():
    spark = SparkSession.builder \
        .appName("FeatureEngineeringJob") \
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

    print("Loading raw events from Delta Lake...")
    df = spark.read.format("delta").load("s3a://ecommerce-lakehouse/events_raw/")

    # Drop invalid sessions
    df = df.filter(col("user_session").isNotNull() & (col("user_session") != ""))

    # Convert event_time to unix seconds for easy duration calculation
    # Format matches: "2019-10-01 02:46:51 UTC"
    df = df.withColumn("event_time_sec", unix_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss 'UTC'"))

    print("Engineering features (sessionization, aggregations, labels)...")
    features_df = df.groupBy("user_session").agg(
        _sum(when(col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
        _sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("cart_count"),
        _sum(col("price")).alias("total_session_value"),
        (_max(col("event_time_sec")) - _min(col("event_time_sec"))).alias("session_duration_seconds"),
        _max(when(col("event_type") == "purchase", 1).otherwise(0)).alias("label")
    )

    # Handle null values that could result from aggregations
    features_df = features_df.fillna(0)

    print("Saving prepared features to Delta Lake...")
    features_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save("s3a://ecommerce-lakehouse/features_prepared/")

    print("Feature engineering completed! Verifying schema and top 5 rows:")
    final_df = spark.read.format("delta").load("s3a://ecommerce-lakehouse/features_prepared/")
    final_df.printSchema()
    final_df.show(5, truncate=False)

if __name__ == "__main__":
    main()
