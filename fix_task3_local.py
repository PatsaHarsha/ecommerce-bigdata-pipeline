import json
import os

def use_local_spark():
    path = 'notebooks/task3_my_dataset.ipynb'
    with open(path, 'r', encoding='utf-8') as f:
        nb = json.load(f)
    
    for cell in nb['cells']:
        if cell['cell_type'] == 'code':
            source = "".join(cell['source'])
            if 'spark = SparkSession.builder' in source:
                cell['source'] = [
                    "from pyspark.sql import SparkSession\n",
                    "from pyspark.sql.functions import col, avg, count\n",
                    "import pandas as pd\n",
                    "import os\n",
                    "\n",
                    "spark = SparkSession.builder \\\n",
                    "    .appName(\"DiamondsAnalysis\") \\\n",
                    "    .master(\"local[*]\") \\\n",
                    "    .config(\"spark.hadoop.fs.s3a.endpoint\", \"http://minio:9000\") \\\n",
                    "    .config(\"spark.hadoop.fs.s3a.access.key\", \"admin\") \\\n",
                    "    .config(\"spark.hadoop.fs.s3a.secret.key\", \"bigdata123\") \\\n",
                    "    .config(\"spark.hadoop.fs.s3a.path.style.access\", \"true\") \\\n",
                    "    .config(\"spark.hadoop.fs.s3a.impl\", \"org.apache.hadoop.fs.s3a.S3AFileSystem\") \\\n",
                    "    .config(\"spark.sql.extensions\", \"io.delta.sql.DeltaSparkSessionExtension\") \\\n",
                    "    .config(\"spark.sql.catalog.spark_catalog\", \"org.apache.spark.sql.delta.catalog.DeltaCatalog\") \\\n",
                    "    .getOrCreate()\n",
                    "\n",
                    "print(\"Local SparkSession created successfully.\")\n"
                ]
                print("Switched SparkSession to local[*] mode.")

    with open(path, 'w', encoding='utf-8') as f:
        json.dump(nb, f, indent=1)

if __name__ == '__main__':
    use_local_spark()
