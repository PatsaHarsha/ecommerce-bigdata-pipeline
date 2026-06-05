import json

with open('c:/Users/HARSHA/Downloads/infrastructure/infrastructure/notebooks/assignment2.ipynb', 'r') as f:
    notebook = json.load(f)

task5_cells = [
  {
   "cell_type": "markdown",
   "id": "task5-title",
   "metadata": {},
   "source": [
    "## Task 5: Schema and Maintenance"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "id": "task5-enforcement",
   "metadata": {},
   "outputs": [],
   "source": [
    "print(\"5a: Schema Enforcement Test (Attempting to write extra column)\")\n",
    "try:\n",
    "    extra_col_df = df.withColumn(\"extra_metadata\", F.lit(\"test\"))\n",
    "    extra_col_df.write.format(\"delta\").mode(\"append\").save(BRONZE_PATH)\n",
    "except Exception as e:\n",
    "    print(\"--- Caught Expected Error ---\")\n",
    "    print(str(e)[:500] + \"...\")\n",
    "\n",
    "print(\"\\nWhy Delta Lake rejected the write?\")\n",
    "print(\"Delta Lake enforces a strict schema by default to prevent data corruption and ensure downstream pipelines don't break due to unexpected columns.\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "id": "task5-evolution",
   "metadata": {},
   "outputs": [],
   "source": [
    "print(\"5b: Schema Evolution (Adding 'customer_segment' column)\")\n",
    "evolution_df = df.limit(10).withColumn(\"customer_segment\", F.lit(\"VIP\"))\n",
    "\n",
    "evolution_df.write.format(\"delta\").mode(\"append\").option(\"mergeSchema\", \"true\").save(BRONZE_PATH)\n",
    "\n",
    "print(\"Updated Schema:\")\n",
    "spark.read.format(\"delta\").load(BRONZE_PATH).printSchema()\n",
    "\n",
    "print(\"\\nRows with NULL in the new column (old rows):\")\n",
    "spark.read.format(\"delta\").load(BRONZE_PATH).select(\"order_id\", \"customer_segment\").filter(\"customer_segment IS NULL\").show(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "id": "task5-optimize",
   "metadata": {},
   "outputs": [],
   "source": [
    "print(\"5c: OPTIMIZE with ZORDER BY (order_id)\")\n",
    "spark.sql(f\"OPTIMIZE delta.`{BRONZE_PATH}` ZORDER BY (order_id)\")\n",
    "print(\"Optimization complete.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "task6-title",
   "metadata": {},
   "source": [
    "## Task 6: Reflection\n",
    "\n",
    "See `reflection.txt` for the full written response."
   ]
  }
]

notebook['cells'].extend(task5_cells)

with open('c:/Users/HARSHA/Downloads/infrastructure/infrastructure/notebooks/assignment2.ipynb', 'w') as f:
    json.dump(notebook, f, indent=1)
