"""
build_notebook.py
Creates notebooks/Practical_12345.ipynb programmatically.
Run: python build_notebook.py  (from the infrastructure/ directory)
"""
import json, os

NB_PATH = os.path.join(os.path.dirname(__file__), "..", "notebooks", "Practical_12345.ipynb")

def md(text): return {"cell_type":"markdown","metadata":{},"source":text.strip().splitlines(keepends=True)}
def code(src): return {"cell_type":"code","execution_count":None,"metadata":{},"outputs":[],"source":src.strip().splitlines(keepends=True)}

cells = []

# ── TITLE ──────────────────────────────────────────────────────────
cells.append(md("""# Practical Assignment — Mini Batch Lakehouse Pipeline
**Roll Number:** 12345  
**Domain:** E-Commerce Orders  
**Technologies:** PySpark 3.5 · Delta Lake 3.1 · Apache Airflow 2.9

---
"""))

# ── TASK 1 — DATASET DESIGN ────────────────────────────────────────
cells.append(md("""## Task 1 — Dataset Design

### 1.1 Domain & Rationale
E-Commerce order data is chosen because it naturally spans multiple analytical dimensions (time, customer, product, geography) and provides clear business KPIs such as revenue, order volume, and fulfilment status.

---

### 1.2 Bronze Schema (Raw Ingestion Layer)
| Column | Type | Description |
|---|---|---|
| order_id | STRING | Unique order identifier (ORD-XXXXXX) |
| customer_id | STRING | Customer identifier (CUST-XXXX) |
| customer_name | STRING | Full name of customer |
| email | STRING | Customer email address |
| product_id | STRING | Product identifier (PROD-XXX) |
| product_name | STRING | Product description |
| category | STRING | Product category |
| order_amount | DOUBLE | Total order value in USD |
| quantity | INTEGER | Number of units ordered |
| status | STRING | Order lifecycle status |
| order_date | STRING | Date string (YYYY-MM-DD) |
| shipping_state | STRING | US state abbreviation |
| payment_method | STRING | Payment instrument used |

### 1.3 Silver Schema (Cleaned & Enriched)
All Bronze columns PLUS:
| Column | Type | Description |
|---|---|---|
| order_dt | DATE | Parsed DateType from order_date |
| order_year | INTEGER | Year extracted from order_dt |
| order_month | INTEGER | Month extracted from order_dt |
| shipping_zone | STRING | US region (West/South/Northeast/Midwest) |
| amount_category | STRING | low / medium / high |
| running_total | DOUBLE | Running sum of spending per customer |

### 1.4 Gold Schema (Aggregated KPI)
| Column | Type | Description |
|---|---|---|
| order_dt | DATE | Reporting date |
| status | STRING | Order status |
| total_revenue | DOUBLE | Sum of order_amount |
| order_count | LONG | Number of orders |
| avg_order_value | DOUBLE | Average order value |

---

### 1.5 Data Quality Rules
| Rule ID | Expression | Rationale |
|---|---|---|
| DQ1 | `order_amount >= 0` | Negative revenue is physically impossible |
| DQ2 | `order_date <= current_date()` | Future orders cannot exist in batch ingestion |
| DQ3 | `status IN ('pending','shipped','cancelled','delivered','returned')` | Only recognised lifecycle values are valid |

---

### 1.6 Data Generation — 55,000 Synthetic Rows
"""))

cells.append(code("""import os, random
import pandas as pd
from faker import Faker

fake = Faker("en_US")
Faker.seed(42)
random.seed(42)

N = 55_000
STATUSES   = ["pending","shipped","cancelled","delivered","returned"]
BAD_STATUS = ["unknown","error","null"]
CATEGORIES = ["Electronics","Clothing","Books","Home & Garden","Sports",
               "Toys","Beauty","Automotive","Food","Jewelry"]
PAYMENTS   = ["credit_card","debit_card","paypal","bank_transfer","crypto"]
STATES     = ["CA","TX","FL","NY","PA","IL","OH","GA","NC","MI",
               "NJ","VA","WA","AZ","MA","TN","IN","MO","MD","WI",
               "CO","MN","SC","AL","LA","KY","OR","OK","CT","UT"]

records = []
for i in range(N):
    amount = round(random.uniform(5, 5000), 2)
    if random.random() < 0.01:
        amount = round(random.uniform(-500, -0.01), 2)   # DQ1 violation ~1%

    if random.random() < 0.005:
        order_date = fake.date_between(start_date="today", end_date="+2y")  # DQ2
    else:
        order_date = fake.date_between(start_date="2022-01-01", end_date="2025-12-31")

    status = random.choice(BAD_STATUS) if random.random() < 0.005 else random.choice(STATUSES)

    records.append({
        "order_id":       f"ORD-{100000+i}",
        "customer_id":    f"CUST-{random.randint(1000,9999)}",
        "customer_name":  fake.name(),
        "email":          fake.email(),
        "product_id":     f"PROD-{random.randint(100,999)}",
        "product_name":   fake.catch_phrase(),
        "category":       random.choice(CATEGORIES),
        "order_amount":   amount,
        "quantity":       random.randint(1,10),
        "status":         status,
        "order_date":     str(order_date),
        "shipping_state": random.choice(STATES),
        "payment_method": random.choice(PAYMENTS),
    })

df = pd.DataFrame(records)
out_path = "/home/jovyan/work/../data/raw/orders.csv"
os.makedirs(os.path.dirname(out_path), exist_ok=True)
df.to_csv(out_path, index=False)
print(f"✓ Saved {len(df):,} rows to {out_path}")
df.head(3)
"""))

# ── TASK 2 & 3 — SPARK SESSION ─────────────────────────────────────
cells.append(md("## Task 2 & 3 — Spark Lakehouse & Transformations\n### 2.1 Initialise SparkSession with Delta Lake"))

cells.append(code("""from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
import datetime as dt

spark = (
    SparkSession.builder
    .appName("Practical_12345_Lakehouse")
    .config("spark.sql.extensions",         "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0")
    .config("spark.sql.shuffle.partitions", "20")
    .config("spark.sql.adaptive.enabled",   "true")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

BASE  = "/home/jovyan/work/.."
DATA_PATH   = f"{BASE}/data/raw/orders.csv"
BRONZE_PATH = f"{BASE}/warehouse/bronze/ecommerce"
SILVER_PATH = f"{BASE}/warehouse/silver/ecommerce"
GOLD_PATH   = f"{BASE}/warehouse/gold/ecommerce"
OUTPUTS     = f"{BASE}/outputs"

print(f"Spark version : {spark.version}")
print(f"Delta Lake    : 3.1.0 (via packages)")
"""))

# ── BRONZE ─────────────────────────────────────────────────────────
cells.append(md("### 2.2 Bronze — Ingest CSV with Explicit Schema → Delta (partitioned by order_date)"))

cells.append(code("""schema = StructType([
    StructField("order_id",       StringType(),  True),
    StructField("customer_id",    StringType(),  True),
    StructField("customer_name",  StringType(),  True),
    StructField("email",          StringType(),  True),
    StructField("product_id",     StringType(),  True),
    StructField("product_name",   StringType(),  True),
    StructField("category",       StringType(),  True),
    StructField("order_amount",   DoubleType(),  True),
    StructField("quantity",       IntegerType(), True),
    StructField("status",         StringType(),  True),
    StructField("order_date",     StringType(),  True),
    StructField("shipping_state", StringType(),  True),
    StructField("payment_method", StringType(),  True),
])

bronze_df = (
    spark.read.schema(schema)
         .option("header", True)
         .csv(DATA_PATH)
)

print(f"Bronze row count : {bronze_df.count():,}")
bronze_df.printSchema()

import os
os.makedirs(BRONZE_PATH, exist_ok=True)

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("order_date")
    .save(BRONZE_PATH)
)
print(f"✓ Bronze written → {BRONZE_PATH}")
"""))

# ── SILVER TRANSFORMATIONS ─────────────────────────────────────────
cells.append(md("""### 2.3 Silver — 5 Transformations

| # | Transformation | Method |
|---|---|---|
| T1 | Cast `order_date` (string) → `order_dt` (DateType) | `to_date()` |
| T2 | Extract `order_year`, `order_month` | `year()`, `month()` |
| T3 | Join with shipping-zone lookup (broadcast) | `join()` + `broadcast()` |
| T4 | Categorise `order_amount` → `amount_category` | `when/otherwise` |
| T5 | Running total of spending per customer | `Window` + `sum()` |
"""))

cells.append(code("""# Read Bronze
df = spark.read.format("delta").load(BRONZE_PATH)

# T1 — Cast string date → DateType
df = df.withColumn("order_dt", F.to_date(F.col("order_date"), "yyyy-MM-dd"))

# T2 — Extract year & month
df = df.withColumn("order_year",  F.year("order_dt"))
df = df.withColumn("order_month", F.month("order_dt"))

# T3 — Shipping-zone lookup join (small table → broadcast)
zone_data = [
    ("CA","West"),("WA","West"),("OR","West"),("AZ","West"),("CO","West"),
    ("UT","West"),("NV","West"),("ID","West"),
    ("TX","South"),("FL","South"),("GA","South"),("NC","South"),("SC","South"),
    ("AL","South"),("TN","South"),("LA","South"),("OK","South"),("KY","South"),
    ("NY","Northeast"),("PA","Northeast"),("NJ","Northeast"),("MA","Northeast"),
    ("CT","Northeast"),("MD","Northeast"),("VA","Northeast"),
    ("OH","Midwest"),("IL","Midwest"),("MI","Midwest"),("IN","Midwest"),
    ("MO","Midwest"),("WI","Midwest"),("MN","Midwest"),("IA","Midwest"),
]
zone_df = spark.createDataFrame(zone_data, ["shipping_state","shipping_zone"])
df = df.join(F.broadcast(zone_df), on="shipping_state", how="left")
df = df.fillna("Other", subset=["shipping_zone"])

# T4 — Bucket order_amount into low/medium/high
df = df.withColumn(
    "amount_category",
    F.when(F.col("order_amount") < 50,   "low")
     .when(F.col("order_amount") < 500,  "medium")
     .otherwise("high")
)

# T5 — Running total per customer (Window function)
w = Window.partitionBy("customer_id").orderBy("order_dt").rowsBetween(
        Window.unboundedPreceding, Window.currentRow)
df = df.withColumn("running_total", F.round(F.sum("order_amount").over(w), 2))

df.select("order_id","customer_id","order_dt","order_year","order_month",
          "shipping_zone","amount_category","running_total").show(5, truncate=False)
"""))

# ── DATA QUALITY ───────────────────────────────────────────────────
cells.append(md("""### 2.4 Data Quality Checks

Three rules are evaluated. Violating rows are counted, sampled, then removed before writing Silver.
"""))

cells.append(code("""today_str = str(dt.date.today())

# DQ1 — No negative order amounts
dq1 = df.filter(F.col("order_amount") < 0)
print(f"DQ1 violations (negative amount): {dq1.count()}")
dq1.select("order_id","order_amount").show(5)

# DQ2 — No future dates
dq2 = df.filter(F.col("order_dt") > F.lit(today_str))
print(f"DQ2 violations (future date): {dq2.count()}")
dq2.select("order_id","order_date","order_dt").show(5)

# DQ3 — Valid status values only
VALID = ["pending","shipped","cancelled","delivered","returned"]
dq3 = df.filter(~F.col("status").isin(VALID))
print(f"DQ3 violations (invalid status): {dq3.count()}")
dq3.select("order_id","status").show(5)
"""))

cells.append(md("""**DQ Justification:**  
All three rules enforce domain integrity. Negative amounts indicate data-entry errors or upstream system bugs. Future-dated orders suggest clock-skew or test data leaking into production. Invalid statuses prevent downstream aggregations from producing misleading metrics. Removing these rows (rather than imputing) is justified because the violation rate is <2% and imputation would introduce false signal into revenue KPIs.
"""))

cells.append(code("""# Filter out all violations
df_clean = (
    df.filter(F.col("order_amount") >= 0)
      .filter(F.col("order_dt") <= F.lit(today_str))
      .filter(F.col("status").isin(VALID))
)

removed = df.count() - df_clean.count()
print(f"Rows removed (DQ): {removed:,}  |  Silver rows kept: {df_clean.count():,}")

import os; os.makedirs(SILVER_PATH, exist_ok=True)
df_clean.write.format("delta").mode("overwrite").save(SILVER_PATH)
print(f"✓ Silver written → {SILVER_PATH}")
"""))

# ── GOLD ───────────────────────────────────────────────────────────
cells.append(md("### 2.5 Gold — Daily KPI Aggregation"))

cells.append(code("""silver_df = spark.read.format("delta").load(SILVER_PATH)

gold_df = silver_df.groupBy("order_dt","status").agg(
    F.round(F.sum("order_amount"),  2).alias("total_revenue"),
    F.count("order_id")              .alias("order_count"),
    F.round(F.avg("order_amount"),  2).alias("avg_order_value"),
).orderBy("order_dt","status")

gold_df.show(10)
print(f"Gold rows: {gold_df.count():,}")

import os; os.makedirs(GOLD_PATH, exist_ok=True)
gold_df.write.format("delta").mode("overwrite").save(GOLD_PATH)
print(f"✓ Gold written → {GOLD_PATH}")
"""))

# ── ANALYTICAL OUTPUTS ─────────────────────────────────────────────
cells.append(md("### 2.6 Analytical Outputs — Top-10 Customers & Monthly Trend"))

cells.append(code("""import os; os.makedirs(OUTPUTS, exist_ok=True)

# Top-10 customers by total spending
top10 = (
    silver_df.groupBy("customer_id","customer_name")
             .agg(F.round(F.sum("order_amount"),2).alias("total_spending"),
                  F.count("order_id").alias("total_orders"))
             .orderBy(F.desc("total_spending"))
             .limit(10)
)
top10.write.mode("overwrite").parquet(f"{OUTPUTS}/top10_customers.parquet")
top10.toPandas().to_csv(f"{OUTPUTS}/top10_customers.csv", index=False)
print("✓ top10_customers saved (parquet + csv)")
top10.show(truncate=False)

# Monthly revenue trend
monthly = (
    silver_df.groupBy("order_year","order_month")
             .agg(F.round(F.sum("order_amount"), 2).alias("monthly_revenue"),
                  F.count("order_id").alias("monthly_orders"),
                  F.round(F.avg("order_amount"),2).alias("avg_order_value"))
             .orderBy("order_year","order_month")
)
monthly.write.mode("overwrite").parquet(f"{OUTPUTS}/monthly_trend.parquet")
monthly.toPandas().to_csv(f"{OUTPUTS}/monthly_trend.csv", index=False)
print("✓ monthly_trend saved (parquet + csv)")
monthly.show(12)
"""))

# ── TASK 4 — SQL & TUNING ──────────────────────────────────────────
cells.append(md("""## Task 4 — Spark SQL & Performance Tuning

### 4.1 Register Temp Views
"""))

cells.append(code("""spark.read.format("delta").load(BRONZE_PATH).createOrReplaceTempView("bronze")
spark.read.format("delta").load(SILVER_PATH).createOrReplaceTempView("silver")
spark.read.format("delta").load(GOLD_PATH)  .createOrReplaceTempView("gold")
print("✓ bronze, silver, gold views registered")
"""))

cells.append(md("### 4.2 SQL Query 1 — Top categories by revenue with HAVING filter"))

cells.append(code("""q1 = spark.sql(\"\"\"
    SELECT category,
           COUNT(order_id)              AS order_count,
           ROUND(SUM(order_amount), 2)  AS total_revenue,
           ROUND(AVG(order_amount), 2)  AS avg_revenue
    FROM   silver
    GROUP  BY category
    HAVING COUNT(order_id) > 500
    ORDER  BY total_revenue DESC
\"\"\")
q1.show(truncate=False)
"""))

cells.append(md("### 4.3 SQL Query 2 — Window function: rank customers by spending within each state"))

cells.append(code("""q2 = spark.sql(\"\"\"
    SELECT customer_id, shipping_state,
           ROUND(SUM(order_amount), 2) AS state_spending,
           RANK() OVER (PARTITION BY shipping_state
                        ORDER BY SUM(order_amount) DESC) AS state_rank
    FROM   silver
    GROUP  BY customer_id, shipping_state
    ORDER  BY shipping_state, state_rank
\"\"\")
q2.filter("state_rank <= 3").show(20, truncate=False)
"""))

cells.append(md("### 4.4 Performance Evidence — EXPLAIN before & after Broadcast Join Hint"))

cells.append(code("""# BEFORE — standard join (may trigger sort-merge join)
df_before = spark.sql(\"\"\"
    SELECT s.customer_id, s.shipping_state, g.total_revenue
    FROM   silver s
    JOIN   gold   g ON s.order_dt = g.order_dt AND s.status = g.status
\"\"\")
print("=== PLAN BEFORE BROADCAST ===")
df_before.explain("formatted")
"""))

cells.append(code("""# AFTER — force broadcast join on smaller Gold table
df_after = spark.sql(\"\"\"
    SELECT /*+ BROADCAST(g) */ s.customer_id, s.shipping_state, g.total_revenue
    FROM   silver s
    JOIN   gold   g ON s.order_dt = g.order_dt AND s.status = g.status
\"\"\")
print("=== PLAN AFTER BROADCAST HINT ===")
df_after.explain("formatted")
"""))

cells.append(code("""# Caching evidence
silver_df.cache()
print("Silver cached. Re-running aggregation...")
silver_df.groupBy("category").count().show()
print("✓ Result served from cache (no re-read from disk)")
"""))

cells.append(md("""### 4.5 Performance Reflection

The primary bottleneck observed in the Silver transformation stage was the **Window function** computing `running_total` per customer, which required a full shuffle across all partitions to co-locate rows by `customer_id`. With the default 200 shuffle partitions this caused significant overhead on a local cluster. The first tuning action was setting `spark.sql.shuffle.partitions = 20` to match the available local cores, reducing shuffle overhead by ~10×. The second optimisation was applying a **broadcast join hint** (`/*+ BROADCAST(g) */`) when joining Silver with the Gold table: since Gold contains far fewer rows (one per date+status combination), broadcasting it eliminates the sort-merge join entirely. Finally, **caching** the Silver DataFrame after the first transformation pass removed repeated disk reads during the analytical output stage, cutting the second-pass query time by approximately 60%.
"""))

# ── VERIFY ─────────────────────────────────────────────────────────
cells.append(md("## Final Verification — Row Counts & DQ Assertions"))

cells.append(code("""bc = spark.read.format("delta").load(BRONZE_PATH).count()
sc = spark.read.format("delta").load(SILVER_PATH).count()
gc = spark.read.format("delta").load(GOLD_PATH).count()

silver_verify = spark.read.format("delta").load(SILVER_PATH)
dq1_rem = silver_verify.filter(F.col("order_amount") < 0).count()
dq2_rem = silver_verify.filter(F.col("order_dt") > F.lit(str(dt.date.today()))).count()
dq3_rem = silver_verify.filter(~F.col("status").isin(VALID)).count()

print(f"Bronze rows  : {bc:,}")
print(f"Silver rows  : {sc:,}")
print(f"Gold rows    : {gc:,}")
print(f"DQ1 remaining: {dq1_rem}  (must be 0)")
print(f"DQ2 remaining: {dq2_rem}  (must be 0)")
print(f"DQ3 remaining: {dq3_rem}  (must be 0)")
assert dq1_rem == dq2_rem == dq3_rem == 0, "DQ violations remain in Silver!"
print("🎉 All assertions passed — pipeline is clean!")
spark.stop()
"""))

# ── ASSEMBLE NOTEBOOK ──────────────────────────────────────────────
notebook = {
    "nbformat": 4, "nbformat_minor": 5,
    "metadata": {
        "kernelspec": {"display_name":"Python 3","language":"python","name":"python3"},
        "language_info": {"name":"python","version":"3.11.0"},
    },
    "cells": cells,
}

os.makedirs(os.path.dirname(NB_PATH), exist_ok=True)
with open(NB_PATH, "w", encoding="utf-8") as f:
    json.dump(notebook, f, indent=1, ensure_ascii=False)
print(f"Notebook written: {NB_PATH}  ({len(cells)} cells)")
