import json

notebook = {
 "cells": [
  {
   "cell_type": "markdown",
   "id": "task1-title",
   "metadata": {},
   "source": [
    "# Z5008 Big Data Lab — Assignment 2\n",
    "**Student Name: [Your Name] | Roll Number: [Your Roll Number]**\n",
    "**Tools used: Antigravity AI**\n",
    "\n",
    "---\n",
    "\n",
    "## Task 1: Environment Setup and Verification"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "id": "task1-spark",
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.sql import SparkSession\n",
    "from delta import configure_spark_with_delta_pip\n",
    "\n",
    "builder = (\n",
    "    SparkSession.builder\n",
    "    .appName(\"Z5008-Assign2-ECommerce\")\n",
    "    .config(\"spark.sql.extensions\", \"io.delta.sql.DeltaSparkSessionExtension\")\n",
    "    .config(\"spark.sql.catalog.spark_catalog\", \"org.apache.spark.sql.delta.catalog.DeltaCatalog\")\n",
    "    .config(\"spark.hadoop.fs.s3a.endpoint\",            \"http://minio:9000\")\n",
    "    .config(\"spark.hadoop.fs.s3a.access.key\",          \"admin\")\n",
    "    .config(\"spark.hadoop.fs.s3a.secret.key\",          \"bigdata123\")\n",
    "    .config(\"spark.hadoop.fs.s3a.path.style.access\",   \"true\")\n",
    "    .config(\"spark.hadoop.fs.s3a.impl\",                \"org.apache.hadoop.fs.s3a.S3AFileSystem\")\n",
    "    .config(\"spark.hadoop.fs.s3a.connection.ssl.enabled\", \"false\")\n",
    "    .config(\"spark.sql.adaptive.enabled\",       \"true\")\n",
    "    .config(\"spark.sql.shuffle.partitions\",     \"8\")\n",
    ")\n",
    "\n",
    "spark = configure_spark_with_delta_pip(\n",
    "    builder, \n",
    "    extra_packages=[\"org.apache.hadoop:hadoop-aws:3.3.4\", \"com.amazonaws:aws-java-sdk-bundle:1.12.262\"]\n",
    ").getOrCreate()\n",
    "\n",
    "spark.sparkContext.setLogLevel(\"WARN\")\n",
    "\n",
    "print(\"--- Spark Verification ---\")\n",
    "print(f\"(i)   Spark Version: {spark.version}\")\n",
    "print(f\"(ii)  Default Parallelism: {spark.sparkContext.defaultParallelism}\")\n",
    "print(f\"(iii) Spark Master: {spark.sparkContext.master}\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "task2-title",
   "metadata": {},
   "source": [
    "## Task 2: Write and Query a Delta Lake Table\n",
    "\n",
    "Domain: **E-commerce Transactions in Tanzania**\n",
    "Dataset size: 5,500 rows\n",
    "Partitioned by: `product_category`"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "id": "task2-data",
   "metadata": {},
   "outputs": [],
   "source": [
    "import pandas as pd\n",
    "import random\n",
    "from pyspark.sql.types import (\n",
    "    StructType, StructField, StringType, \n",
    "    DoubleType, TimestampType\n",
    ")\n",
    "from pyspark.sql import functions as F\n",
    "\n",
    "random.seed(42)\n",
    "n = 5500\n",
    "\n",
    "categories = [\"Electronics\", \"Fashion\", \"Home & Garden\", \"Beauty\", \"Sports\", \"Groceries\"]\n",
    "payment_methods = [\"M-Pesa\", \"Tigo Pesa\", \"Airtel Money\", \"Credit Card\", \"Cash\"]\n",
    "regions = [\"Dar es Salaam\", \"Arusha\", \"Mwanza\", \"Dodoma\", \"Zanzibar City\"]\n",
    "\n",
    "pdf = pd.DataFrame({\n",
    "    \"order_id\":      [f\"ORD-{i+10000:05d}\" for i in range(n)],\n",
    "    \"customer_id\":   [f\"CUST-{random.randint(1, 1000):04d}\" for i in range(n)],\n",
    "    \"order_date\":    pd.date_range(\"2026-01-01\", periods=n, freq=\"15min\"),\n",
    "    \"product_category\": [random.choice(categories) for _ in range(n)],\n",
    "    \"amount_tzs\":    [round(random.uniform(5000, 500000), 2) for _ in range(n)],\n",
    "    \"payment_method\": [random.choice(payment_methods) for _ in range(n)],\n",
    "    \"region\":        [random.choice(regions) for _ in range(n)]\n",
    "})\n",
    "\n",
    "schema = StructType([\n",
    "    StructField(\"order_id\", StringType(), False),\n",
    "    StructField(\"customer_id\", StringType(), True),\n",
    "    StructField(\"order_date\", TimestampType(), True),\n",
    "    StructField(\"product_category\", StringType(), True),\n",
    "    StructField(\"amount_tzs\", DoubleType(), True),\n",
    "    StructField(\"payment_method\", StringType(), True),\n",
    "    StructField(\"region\", StringType(), True)\n",
    "])\n",
    "\n",
    "df = spark.createDataFrame(pdf, schema=schema)\n",
    "print(f\"Generated {df.count()} rows in E-commerce domain.\")\n",
    "\n",
    "BRONZE_PATH = \"s3a://warehouse/bronze/ecommerce_trx\"\n",
    "\n",
    "df.write.format(\"delta\").mode(\"overwrite\").partitionBy(\"product_category\").save(BRONZE_PATH)\n",
    "print(f\"Table written to {BRONZE_PATH}\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "id": "task2-queries",
   "metadata": {},
   "outputs": [],
   "source": [
    "df_bronze = spark.read.format(\"delta\").load(BRONZE_PATH)\n",
    "df_bronze.createOrReplaceTempView(\"ecommerce_trx\")\n",
    "\n",
    "print(\"Query 1: Total Revenue and Average Order Value by Product Category\")\n",
    "spark.sql(\"\"\"\n",
    "    SELECT product_category, \n",
    "           COUNT(*) as order_count, \n",
    "           ROUND(SUM(amount_tzs), 2) as total_revenue, \n",
    "           ROUND(AVG(amount_tzs), 2) as avg_order_value\n",
    "    FROM ecommerce_trx\n",
    "    GROUP BY product_category\n",
    "    ORDER BY total_revenue DESC\n",
    "\"\"\").show()\n",
    "\n",
    "print(\"Query 2: Payment Method Usage Analysis\")\n",
    "spark.sql(\"\"\"\n",
    "    SELECT payment_method, \n",
    "           COUNT(*) as usage_count, \n",
    "           ROUND(SUM(amount_tzs), 2) as total_processed\n",
    "    FROM ecommerce_trx\n",
    "    GROUP BY payment_method\n",
    "    ORDER BY usage_count DESC\n",
    "\"\"\").show()\n",
    "\n",
    "print(\"Query 3: Regional Sales Performance\")\n",
    "spark.sql(\"\"\"\n",
    "    SELECT region, \n",
    "           COUNT(*) as order_count, \n",
    "           ROUND(SUM(amount_tzs), 2) as total_sales, \n",
    "           MAX(amount_tzs) as largest_order\n",
    "    FROM ecommerce_trx\n",
    "    GROUP BY region\n",
    "    ORDER BY total_sales DESC\n",
    "\"\"\").show()"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.7"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}

with open('c:/Users/HARSHA/Downloads/infrastructure/infrastructure/notebooks/assignment2.ipynb', 'w') as f:
    json.dump(notebook, f, indent=1)
