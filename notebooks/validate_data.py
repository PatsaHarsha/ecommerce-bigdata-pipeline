from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ValidateEcommerceData") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Reading data from s3a://warehouse/events...")
try:
    df = spark.read.format("delta").load("s3a://warehouse/events")
    count = df.count()
    print(f"\n====================================")
    print(f"SUCCESS! Total records in Delta Lake: {count}")
    print(f"====================================\n")
    df.show(5)
except Exception as e:
    print(f"Failed to read Delta table: {e}")
