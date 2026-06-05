"""
Practical Assignment — Mini Batch Lakehouse Pipeline DAG
Roll Number: 12345

DAG: practical_12345
Schedule: @once
Tasks:
  1. generate_raw_data   — Generate 55,000 synthetic e-commerce orders
  2. ingest_to_bronze    — Read CSV, write Delta Lake bronze table
  3. transform_to_silver — Apply 5 transformations + DQ checks, write silver
  4. build_gold          — Aggregate KPIs, write gold table
  5. verify_pipeline     — Assert row counts > 0, 0 DQ violations remaining
"""

from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ------------------------------------------------------------------ #
# Paths — adjusted so they work inside the Airflow container          #
# The dags/ folder is mounted at /opt/airflow/dags inside the         #
# container; data & warehouse folders sit one level up.               #
# ------------------------------------------------------------------ #
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_RAW_PATH = os.path.join(BASE_DIR, "data", "raw", "orders.csv")
BRONZE_PATH = os.path.join(BASE_DIR, "warehouse", "bronze", "ecommerce")
SILVER_PATH = os.path.join(BASE_DIR, "warehouse", "silver", "ecommerce")
GOLD_PATH = os.path.join(BASE_DIR, "warehouse", "gold", "ecommerce")
OUTPUTS_PATH = os.path.join(BASE_DIR, "outputs")

log = logging.getLogger(__name__)

# ================================================================== #
# DEFAULT ARGS                                                        #
# ================================================================== #
default_args = {
    "owner": "student_12345",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
    "email_on_retry": False,
}

# ================================================================== #
# DAG DEFINITION                                                      #
# ================================================================== #
with DAG(
    dag_id="practical_12345",
    default_args=default_args,
    description="Mini Batch Lakehouse: Bronze → Silver → Gold pipeline (Roll: 12345)",
    schedule_interval="@once",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["practical"],
    params={"n_rows": 55000},
) as dag:

    # ============================================================== #
    # TASK 1 — Generate Raw Data                                     #
    # ============================================================== #
    def _generate_raw_data(**context):
        """Generate synthetic e-commerce orders using Faker and save as CSV."""
        import subprocess, sys
        subprocess.check_call([sys.executable, "-m", "pip", "install", "faker", "pandas", "--quiet"])
        
        import random
        import pandas as pd
        from faker import Faker

        n_rows = context["params"].get("n_rows", 55000)
        fake = Faker("en_US")
        Faker.seed(42)
        random.seed(42)

        STATUSES = ["pending", "shipped", "cancelled", "delivered", "returned"]
        INVALID_STATUSES = ["unknown", "error", "null"]  # ~1% DQ violations
        CATEGORIES = [
            "Electronics", "Clothing", "Books", "Home & Garden",
            "Sports", "Toys", "Beauty", "Automotive", "Food", "Jewelry",
        ]
        PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "bank_transfer", "crypto"]
        US_STATES = [
            "CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI",
            "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI",
            "CO", "MN", "SC", "AL", "LA", "KY", "OR", "OK", "CT", "UT",
        ]

        records = []
        for i in range(n_rows):
            # Inject ~2% DQ violations deliberately
            amount = round(random.uniform(-10, 5000), 2)
            if random.random() < 0.01:  # ~1% negative (DQ1 violation)
                amount = round(random.uniform(-500, -0.01), 2)

            # DQ2: ~0.5% future dates
            if random.random() < 0.005:
                order_date = fake.date_between(start_date="today", end_date="+2y")
            else:
                order_date = fake.date_between(start_date="2022-01-01", end_date="2025-12-31")

            # DQ3: ~0.5% invalid status
            if random.random() < 0.005:
                status = random.choice(INVALID_STATUSES)
            else:
                status = random.choice(STATUSES)

            records.append({
                "order_id": f"ORD-{100000 + i}",
                "customer_id": f"CUST-{random.randint(1000, 9999)}",
                "customer_name": fake.name(),
                "email": fake.email(),
                "product_id": f"PROD-{random.randint(100, 999)}",
                "product_name": fake.catch_phrase(),
                "category": random.choice(CATEGORIES),
                "order_amount": amount,
                "quantity": random.randint(1, 10),
                "status": status,
                "order_date": str(order_date),
                "shipping_state": random.choice(US_STATES),
                "payment_method": random.choice(PAYMENT_METHODS),
            })

        df = pd.DataFrame(records)
        os.makedirs(os.path.dirname(DATA_RAW_PATH), exist_ok=True)
        df.to_csv(DATA_RAW_PATH, index=False)
        log.info(f"Generated {len(df):,} rows → {DATA_RAW_PATH}")
        return len(df)

    # ============================================================== #
    # TASK 2 — Ingest to Bronze (Delta Lake)                         #
    # ============================================================== #
    def _ingest_to_bronze(**context):
        """Read CSV with explicit schema and write as partitioned Delta table."""
        from pyspark.sql import SparkSession
        from pyspark.sql.types import (
            StructType, StructField, StringType, DoubleType, IntegerType
        )

        spark = (
            SparkSession.builder
            .appName("practical_12345_bronze")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.jars.packages",
                    "io.delta:delta-spark_2.12:3.1.0")
            .config("spark.sql.shuffle.partitions", "8")
            .master("local[*]")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("order_amount", DoubleType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("status", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("shipping_state", StringType(), True),
            StructField("payment_method", StringType(), True),
        ])

        df = spark.read.schema(schema).option("header", True).csv(DATA_RAW_PATH)
        row_count = df.count()
        log.info(f"Ingesting {row_count:,} rows to Bronze")

        os.makedirs(BRONZE_PATH, exist_ok=True)
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("order_date")
            .save(BRONZE_PATH)
        )

        log.info(f"Bronze table written → {BRONZE_PATH}")
        spark.stop()
        return row_count

    # ============================================================== #
    # TASK 3 — Transform to Silver                                   #
    # ============================================================== #
    def _transform_to_silver(**context):
        """Apply 5 transformations + 3 DQ checks; write Silver Delta table."""
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        from pyspark.sql.types import StringType
        import datetime as dt

        spark = (
            SparkSession.builder
            .appName("practical_12345_silver")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.jars.packages",
                    "io.delta:delta-spark_2.12:3.1.0")
            .config("spark.sql.shuffle.partitions", "8")
            .master("local[*]")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        # Read Bronze
        df = spark.read.format("delta").load(BRONZE_PATH)

        # ---- TRANSFORMATION 1: Cast order_date string to DateType ---- #
        df = df.withColumn("order_dt", F.to_date(F.col("order_date"), "yyyy-MM-dd"))

        # ---- TRANSFORMATION 2: Extract year and month ---- #
        df = df.withColumn("order_year", F.year(F.col("order_dt")))
        df = df.withColumn("order_month", F.month(F.col("order_dt")))

        # ---- TRANSFORMATION 3: Join with shipping zone lookup ---- #
        zone_data = [
            ("CA", "West"), ("WA", "West"), ("OR", "West"), ("AZ", "West"),
            ("CO", "West"), ("UT", "West"), ("NV", "West"), ("ID", "West"),
            ("TX", "South"), ("FL", "South"), ("GA", "South"), ("NC", "South"),
            ("SC", "South"), ("AL", "South"), ("TN", "South"), ("LA", "South"),
            ("OK", "South"), ("AR", "South"), ("MS", "South"), ("KY", "South"),
            ("NY", "Northeast"), ("PA", "Northeast"), ("NJ", "Northeast"),
            ("MA", "Northeast"), ("CT", "Northeast"), ("MD", "Northeast"),
            ("VA", "Northeast"), ("NH", "Northeast"), ("VT", "Northeast"),
            ("OH", "Midwest"), ("IL", "Midwest"), ("MI", "Midwest"),
            ("IN", "Midwest"), ("MO", "Midwest"), ("WI", "Midwest"),
            ("MN", "Midwest"), ("IA", "Midwest"), ("KS", "Midwest"),
        ]
        zone_schema = ["shipping_state", "shipping_zone"]
        zone_df = spark.createDataFrame(zone_data, zone_schema)
        df = df.join(F.broadcast(zone_df), on="shipping_state", how="left")
        df = df.fillna("Other", subset=["shipping_zone"])

        # ---- TRANSFORMATION 4: Categorize order_amount ---- #
        df = df.withColumn(
            "amount_category",
            F.when(F.col("order_amount") < 50, "low")
             .when(F.col("order_amount") < 500, "medium")
             .otherwise("high")
        )

        # ---- TRANSFORMATION 5: Running total per customer (Window) ---- #
        window_spec = (
            Window.partitionBy("customer_id")
                  .orderBy("order_dt")
                  .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        df = df.withColumn("running_total", F.sum("order_amount").over(window_spec))

        # ============================================================ #
        # DATA QUALITY CHECKS                                          #
        # ============================================================ #
        today = dt.date.today()

        # DQ1: order_amount >= 0
        dq1_violations = df.filter(F.col("order_amount") < 0)
        dq1_count = dq1_violations.count()
        log.info(f"DQ1 violations (negative amount): {dq1_count}")

        # DQ2: order_date not in the future
        dq2_violations = df.filter(F.col("order_dt") > F.lit(str(today)))
        dq2_count = dq2_violations.count()
        log.info(f"DQ2 violations (future date): {dq2_count}")

        # DQ3: status in valid values
        valid_statuses = ["pending", "shipped", "cancelled", "delivered", "returned"]
        dq3_violations = df.filter(~F.col("status").isin(valid_statuses))
        dq3_count = dq3_violations.count()
        log.info(f"DQ3 violations (invalid status): {dq3_count}")

        # Filter out all DQ violations
        df_clean = (
            df.filter(F.col("order_amount") >= 0)
              .filter(F.col("order_dt") <= F.lit(str(today)))
              .filter(F.col("status").isin(valid_statuses))
        )

        silver_count = df_clean.count()
        log.info(f"Silver rows after DQ filter: {silver_count:,}")

        os.makedirs(SILVER_PATH, exist_ok=True)
        (
            df_clean.write
            .format("delta")
            .mode("overwrite")
            .save(SILVER_PATH)
        )

        log.info(f"Silver table written → {SILVER_PATH}")
        spark.stop()
        return silver_count

    # ============================================================== #
    # TASK 4 — Build Gold                                            #
    # ============================================================== #
    def _build_gold(**context):
        """Aggregate Silver → Gold KPI table; also produce analytical outputs."""
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = (
            SparkSession.builder
            .appName("practical_12345_gold")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.jars.packages",
                    "io.delta:delta-spark_2.12:3.1.0")
            .config("spark.sql.shuffle.partitions", "8")
            .master("local[*]")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        silver_df = spark.read.format("delta").load(SILVER_PATH)

        # ---- Gold: Daily KPI per status ---- #
        gold_df = silver_df.groupBy("order_dt", "status").agg(
            F.round(F.sum("order_amount"), 2).alias("total_revenue"),
            F.count("order_id").alias("order_count"),
            F.round(F.avg("order_amount"), 2).alias("avg_order_value"),
        ).orderBy("order_dt", "status")

        os.makedirs(GOLD_PATH, exist_ok=True)
        gold_df.write.format("delta").mode("overwrite").save(GOLD_PATH)
        log.info(f"Gold table written → {GOLD_PATH}")

        # ---- Analytical Outputs ---- #
        os.makedirs(OUTPUTS_PATH, exist_ok=True)

        # Top-10 customers by total spending
        top10 = (
            silver_df.groupBy("customer_id", "customer_name")
                     .agg(F.round(F.sum("order_amount"), 2).alias("total_spending"),
                          F.count("order_id").alias("total_orders"))
                     .orderBy(F.desc("total_spending"))
                     .limit(10)
        )
        top10.write.mode("overwrite").parquet(os.path.join(OUTPUTS_PATH, "top10_customers.parquet"))
        top10.toPandas().to_csv(os.path.join(OUTPUTS_PATH, "top10_customers.csv"), index=False)
        log.info("Top-10 customers report saved")

        # Monthly trend report
        monthly = (
            silver_df.groupBy("order_year", "order_month")
                     .agg(F.round(F.sum("order_amount"), 2).alias("monthly_revenue"),
                          F.count("order_id").alias("monthly_orders"),
                          F.round(F.avg("order_amount"), 2).alias("avg_order_value"))
                     .orderBy("order_year", "order_month")
        )
        monthly.write.mode("overwrite").parquet(os.path.join(OUTPUTS_PATH, "monthly_trend.parquet"))
        monthly.toPandas().to_csv(os.path.join(OUTPUTS_PATH, "monthly_trend.csv"), index=False)
        log.info("Monthly trend report saved")

        gold_count = gold_df.count()
        spark.stop()
        return gold_count

    # ============================================================== #
    # TASK 5 — Verify Pipeline                                       #
    # ============================================================== #
    def _verify_pipeline(**context):
        """Assert all Delta tables have rows and no DQ violations remain in Silver."""
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        import datetime as dt

        spark = (
            SparkSession.builder
            .appName("practical_12345_verify")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.jars.packages",
                    "io.delta:delta-spark_2.12:3.1.0")
            .config("spark.sql.shuffle.partitions", "8")
            .master("local[*]")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        bronze_count = spark.read.format("delta").load(BRONZE_PATH).count()
        silver_count = spark.read.format("delta").load(SILVER_PATH).count()
        gold_count = spark.read.format("delta").load(GOLD_PATH).count()

        assert bronze_count > 0, f"FAIL: Bronze table is empty! Count={bronze_count}"
        assert silver_count > 0, f"FAIL: Silver table is empty! Count={silver_count}"
        assert gold_count > 0, f"FAIL: Gold table is empty! Count={gold_count}"

        log.info(f"✓ Bronze rows : {bronze_count:,}")
        log.info(f"✓ Silver rows : {silver_count:,}")
        log.info(f"✓ Gold rows   : {gold_count:,}")

        # Re-verify 0 DQ violations in Silver
        silver_df = spark.read.format("delta").load(SILVER_PATH)
        today = dt.date.today()
        valid_statuses = ["pending", "shipped", "cancelled", "delivered", "returned"]

        dq1 = silver_df.filter(F.col("order_amount") < 0).count()
        dq2 = silver_df.filter(F.col("order_dt") > F.lit(str(today))).count()
        dq3 = silver_df.filter(~F.col("status").isin(valid_statuses)).count()

        assert dq1 == 0, f"FAIL: {dq1} negative amounts in Silver!"
        assert dq2 == 0, f"FAIL: {dq2} future-dated orders in Silver!"
        assert dq3 == 0, f"FAIL: {dq3} invalid statuses in Silver!"

        log.info("✓ DQ1 (no negative amounts) : PASSED")
        log.info("✓ DQ2 (no future dates)     : PASSED")
        log.info("✓ DQ3 (valid statuses only) : PASSED")
        log.info("🎉 Pipeline verification PASSED — all assertions satisfied!")

        spark.stop()
        return {
            "bronze_count": bronze_count,
            "silver_count": silver_count,
            "gold_count": gold_count,
            "dq_violations": 0,
        }

    # ============================================================== #
    # WIRE UP TASKS                                                   #
    # ============================================================== #
    t_generate = PythonOperator(
        task_id="generate_raw_data",
        python_callable=_generate_raw_data,
    )

    t_bronze = PythonOperator(
        task_id="ingest_to_bronze",
        python_callable=_ingest_to_bronze,
    )

    t_silver = PythonOperator(
        task_id="transform_to_silver",
        python_callable=_transform_to_silver,
    )

    t_gold = PythonOperator(
        task_id="build_gold",
        python_callable=_build_gold,
    )

    t_verify = PythonOperator(
        task_id="verify_pipeline",
        python_callable=_verify_pipeline,
    )

    # Dependency chain
    t_generate >> t_bronze >> t_silver >> t_gold >> t_verify
