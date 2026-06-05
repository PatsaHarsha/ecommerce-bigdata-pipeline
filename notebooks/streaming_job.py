from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import time

print("Starting Spark Session for Kafka Streaming...")
spark = SparkSession.builder \
    .appName("EcommerceKafkaStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define the schema matching our producer
schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("price", DoubleType(), True)
])

# Read from Kafka. Remember, internal port is 29092
print("Connecting to Kafka...")
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:29092") \
  .option("subscribe", "user_events") \
  .option("startingOffsets", "earliest") \
  .load()

# Parse the JSON from the 'value' column
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Write to Delta Lake
print("Starting write stream to MinIO Delta Lake...")
query = parsed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://warehouse/events/_checkpoints/streaming") \
    .start("s3a://warehouse/events")

# Keep the stream running for a bit, then stop gracefully for the demo milestone
print("Streaming active... letting it run for 30 seconds.")
time.sleep(30)
query.stop()
print("Streaming job stopped successfully.")
