
import time
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Monitor").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
for i in range(4):
    try:
        count = spark.read.format("delta").load("s3a://warehouse/events").count()
        print(f"[{time.strftime('%H:%M:%S')}] Active Stream writing... Total records in Delta Lake: {count}")
    except:
        pass
    time.sleep(3)
