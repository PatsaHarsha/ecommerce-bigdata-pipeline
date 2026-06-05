from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

def create_spark_session():
    return SparkSession.builder \
        .appName("KafkaToDeltaStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "bigdata123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

def main():
    print("🚀 Initializing Spark Session...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Schema Definition explicitly mapping the Kafka JSON structure
    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", LongType(), True),
        StructField("category_id", StringType(), True),
        StructField("user_id", LongType(), True),
        StructField("user_session", StringType(), True),
        StructField("price", DoubleType(), True)
    ])

    # 2. Kafka Ingestion
    print("📡 Connecting to Kafka topic 'ecommerce_events'...")
    # NOTE: Using kafka:9092 because the Spark job will run inside the Docker network.
    # If testing strictly from the host laptop, this would be localhost:29092
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "ecommerce_events") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse the binary JSON payload into defined columns
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # 3. Delta Sink to MinIO
    print("💾 Starting streaming write to MinIO Delta Lake (s3a://ecommerce-lakehouse/events_raw/)...")
    query = parsed_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://ecommerce-lakehouse/checkpoints/events_raw/") \
        .start("s3a://ecommerce-lakehouse/events_raw/")

    # Keep the streaming job running
    query.awaitTermination()

if __name__ == "__main__":
    main()
