import json

with open('c:/Users/HARSHA/Downloads/infrastructure/infrastructure/notebooks/assignment2.ipynb', 'r') as f:
    notebook = json.load(f)

# Task 3 Cells
task3_cells = [
  {
   "cell_type": "markdown",
   "id": "task3-title",
   "metadata": {},
   "source": [
    "## Task 3: Time Travel"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "id": "task3-versions",
   "metadata": {},
   "outputs": [],
   "source": [
    "BRONZE_PATH = \"s3a://warehouse/bronze/ecommerce_trx\"\n",
    "\n",
    "# Version 1: Append 500 new rows\n",
    "pdf_v1 = pd.DataFrame({\n",
    "    \"order_id\":      [f\"ORD-{i+16000:05d}\" for i in range(500)],\n",
    "    \"customer_id\":   [f\"CUST-{random.randint(1, 1000):04d}\" for i in range(500)],\n",
    "    \"order_date\":    pd.date_range(\"2026-03-01\", periods=500, freq=\"h\"),\n",
    "    \"product_category\": [random.choice(categories) for _ in range(500)],\n",
    "    \"amount_tzs\":    [round(random.uniform(10000, 200000), 2) for _ in range(500)],\n",
    "    \"payment_method\": [random.choice(payment_methods) for _ in range(500)],\n",
    "    \"region\":        [random.choice(regions) for _ in range(500)]\n",
    "})\n",
    "df_v1 = spark.createDataFrame(pdf_v1, schema=schema)\n",
    "df_v1.write.format(\"delta\").mode(\"append\").save(BRONZE_PATH)\n",
    "print(\"Version 1 created (Append 500 rows).\")\n",
    "\n",
    "# Version 2: Append 100 more rows\n",
    "pdf_v2 = pd.DataFrame({\n",
    "    \"order_id\":      [f\"ORD-{i+17000:05d}\" for i in range(100)],\n",
    "    \"customer_id\":   [f\"CUST-{random.randint(1, 1000):04d}\" for i in range(100)],\n",
    "    \"order_date\":    pd.date_range(\"2026-03-22\", periods=10, freq=\"min\").tolist() * 10,\n",
    "    \"product_category\": [random.choice(categories) for _ in range(100)],\n",
    "    \"amount_tzs\":    [50000.0] * 100,\n",
    "    \"payment_method\": [\"M-Pesa\"] * 100,\n",
    "    \"region\":        [\"Dar es Salaam\"] * 100\n",
    "})\n",
    "df_v2 = spark.createDataFrame(pdf_v2, schema=schema)\n",
    "df_v2.write.format(\"delta\").mode(\"append\").save(BRONZE_PATH)\n",
    "print(\"Version 2 created (Append 100 rows).\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "id": "task3-history",
   "metadata": {},
   "outputs": [],
   "source": [
    "print(\"--- Table History ---\")\n",
    "display(spark.sql(f\"DESCRIBE HISTORY delta.`{BRONZE_PATH}`\").select(\"version\", \"timestamp\", \"operation\", \"operationMetrics\"))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "id": "task3-compare",
   "metadata": {},
   "outputs": [],
   "source": [
    "v0_df = spark.read.format(\"delta\").option(\"versionAsOf\", 0).load(BRONZE_PATH)\n",
    "v2_df = spark.read.format(\"delta\").option(\"versionAsOf\", 2).load(BRONZE_PATH)\n",
    "\n",
    "v0_count = v0_df.count()\n",
    "v0_sum = v0_df.select(F.sum(\"amount_tzs\")).collect()[0][0]\n",
    "v2_count = v2_df.count()\n",
    "v2_sum = v2_df.select(F.sum(\"amount_tzs\")).collect()[0][0]\n",
    "\n",
    "print(f\"Version 0: {v0_count} rows, total = {v0_sum:,.2f} TZS\")\n",
    "print(f\"Version 2: {v2_count} rows, total = {v2_sum:,.2f} TZS\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "id": "task3-restore",
   "metadata": {},
   "outputs": [],
   "source": [
    "print(\"Simulating accident and recovering...\")\n",
    "spark.sql(f\"RESTORE TABLE delta.`{BRONZE_PATH}` TO VERSION AS OF 0\")\n",
    "\n",
    "restored_count = spark.read.format(\"delta\").load(BRONZE_PATH).count()\n",
    "print(f\"Verified: Row count is back to {restored_count} (Version 0 count).\")\n",
    "print(\"\\nWhy RESTORE is more reliable than file system backup?\")\n",
    "print(\"RESTORE is atomic and uses the transaction log to guarantee consistency, whereas file system backups might capture partial writes or inconsistent states.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "task4-title",
   "metadata": {},
   "source": [
    "## Task 4: MERGE / UPSERT"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "id": "task4-silver",
   "metadata": {},
   "outputs": [],
   "source": [
    "SILVER_PATH = \"s3a://warehouse/silver/ecommerce_summary\"\n",
    "\n",
    "print(\"Creating Silver summary table...\")\n",
    "(\n",
    "    df_bronze.groupBy(\"customer_id\")\n",
    "    .agg(\n",
    "        F.round(F.sum(\"amount_tzs\"), 2).alias(\"total_spend\"),\n",
    "        F.count(\"*\").alias(\"order_count\"),\n",
    "        F.max(\"order_date\").alias(\"last_activity_date\"),\n",
    "        F.lit(\"active\").alias(\"status\")\n",
    "    )\n",
    "    .write.format(\"delta\").mode(\"overwrite\").save(SILVER_PATH)\n",
    ")\n",
    "print(f\"Silver table written to {SILVER_PATH}\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "id": "task4-prepare",
   "metadata": {},
   "outputs": [],
   "source": [
    "from delta.tables import DeltaTable\n",
    "\n",
    "# Get some existing IDs for updates and deletes\n",
    "existing_customers = spark.read.format(\"delta\").load(SILVER_PATH).limit(20).collect()\n",
    "update_ids = [r['customer_id'] for r in existing_customers[:10]]\n",
    "delete_ids = [r['customer_id'] for r in existing_customers[10:13]]\n",
    "\n",
    "updates_data = []\n",
    "# 10 Updates\n",
    "for cid in update_ids:\n",
    "    updates_data.append((cid, 1000000.0, 100, pd.Timestamp.now(), \"active\"))\n",
    "# 5 Inserts\n",
    "for i in range(5):\n",
    "    updates_data.append((f\"NEW-{i:03d}\", 50000.0, 1, pd.Timestamp.now(), \"active\"))\n",
    "# 3 Deletes\n",
    "for cid in delete_ids:\n",
    "    updates_data.append((cid, 0.0, 0, pd.Timestamp.now(), \"deleted\"))\n",
    "\n",
    "updates_df = spark.createDataFrame(updates_data, [\"customer_id\", \"total_spend\", \"order_count\", \"last_activity_date\", \"status\"])\n",
    "print(\"Batch of updates prepared.\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "id": "task4-merge",
   "metadata": {},
   "outputs": [],
   "source": [
    "silver_table = DeltaTable.forPath(spark, SILVER_PATH)\n",
    "\n",
    "(\n",
    "    silver_table.alias(\"target\")\n",
    "    .merge(updates_df.alias(\"source\"), \"target.customer_id = source.customer_id\")\n",
    "    .whenMatchedUpdate(condition=\"source.status = 'active'\", set={\n",
    "        \"total_spend\": \"source.total_spend\",\n",
    "        \"order_count\": \"source.order_count\",\n",
    "        \"last_activity_date\": \"source.last_activity_date\"\n",
    "    })\n",
    "    .whenMatchedDelete(condition=\"source.status = 'deleted'\")\n",
    "    .whenNotMatchedInsert(values={\n",
    "        \"customer_id\": \"source.customer_id\",\n",
    "        \"total_spend\": \"source.total_spend\",\n",
    "        \"order_count\": \"source.order_count\",\n",
    "        \"last_activity_date\": \"source.last_activity_date\",\n",
    "        \"status\": \"source.status\"\n",
    "    })\n",
    "    .execute()\n",
    ")\n",
    "print(\"MERGE operation completed.\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "id": "task4-verify",
   "metadata": {},
   "outputs": [],
   "source": [
    "df_final = spark.read.format(\"delta\").load(SILVER_PATH)\n",
    "\n",
    "print(\"Verifying Updates:\")\n",
    "df_final.filter(F.col(\"customer_id\").isin(update_ids)).show()\n",
    "\n",
    "print(\"Verifying Deletes (should be 0 rows):\")\n",
    "print(f\"Count of deleted IDs: {df_final.filter(F.col(\"customer_id\").isin(delete_ids)).count()}\")\n",
    "\n",
    "print(\"Verifying Inserts:\")\n",
    "df_final.filter(F.col(\"customer_id\").startswith(\"NEW-\")).show()\n",
    "\n",
    "print(\"--- Merge Recorded in History ---\")\n",
    "display(spark.sql(f\"DESCRIBE HISTORY delta.`{SILVER_PATH}`\").select(\"version\", \"operation\").limit(5))"
   ]
  }
]

notebook['cells'].extend(task3_cells)

with open('c:/Users/HARSHA/Downloads/infrastructure/infrastructure/notebooks/assignment2.ipynb', 'w') as f:
    json.dump(notebook, f, indent=1)
