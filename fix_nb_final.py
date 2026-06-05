import json

NB_PATH = 'c:/Users/HARSHA/Downloads/infrastructure/infrastructure/notebooks/assignment2.ipynb'
with open(NB_PATH, 'r') as f:
    notebook = json.load(f)

# Find the cell with Task 1
for cell in notebook['cells']:
    if cell['id'] == 'task1-spark':
        cell['source'] = [
            "from pyspark.sql import SparkSession\n",
            "\n",
            "spark = SparkSession.builder \\\n",
            "    .appName(\"Z5008-Assign2-ECommerce\") \\\n",
            "    .getOrCreate()\n",
            "\n",
            "spark.sparkContext.setLogLevel(\"WARN\")\n",
            "\n",
            "print(\"--- Spark Verification ---\")\n",
            "print(f\"(i)   Spark Version: {spark.version}\")\n",
            "print(f\"(ii)  Default Parallelism: {spark.sparkContext.defaultParallelism}\")\n",
            "print(f\"(iii) Spark Master: {spark.sparkContext.master}\")"
        ]

with open(NB_PATH, 'w') as f:
    json.dump(notebook, f, indent=1)
