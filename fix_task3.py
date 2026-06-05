import json
import os

def fix_task3():
    path = 'notebooks/task3_my_dataset.ipynb'
    with open(path, 'r', encoding='utf-8') as f:
        nb = json.load(f)
    
    for cell in nb['cells']:
        if cell['cell_type'] == 'code':
            source = "".join(cell['source'])
            # Fix local file path to be more robust
            if 'df = spark.read.csv("file:///home/jovyan/work/diamonds.csv"' in source:
                cell['source'] = [s.replace('file:///home/jovyan/work/diamonds.csv', '/home/jovyan/work/diamonds.csv') for s in cell['source']]
                print("Fixed CSV path in load cell.")
                
            # Add explicit session config if needed (though it should be in defaults)
            if 'spark = SparkSession.builder' in source:
                cell['source'] = [
                    "from pyspark.sql import SparkSession\n",
                    "from pyspark.sql.functions import col, avg, count\n",
                    "import pandas as pd\n",
                    "import os\n",
                    "\n",
                    "spark = SparkSession.builder \\\n",
                    "    .appName(\"DiamondsAnalysis\") \\\n",
                    "    .master(\"spark://spark-master:7077\") \\\n",
                    "    .config(\"spark.hadoop.fs.s3a.endpoint\", \"http://minio:9000\") \\\n",
                    "    .config(\"spark.hadoop.fs.s3a.access.key\", \"admin\") \\\n",
                    "    .config(\"spark.hadoop.fs.s3a.secret.key\", \"bigdata123\") \\\n",
                    "    .config(\"spark.hadoop.fs.s3a.path.style.access\", \"true\") \\\n",
                    "    .config(\"spark.hadoop.fs.s3a.impl\", \"org.apache.hadoop.fs.s3a.S3AFileSystem\") \\\n",
                    "    .getOrCreate()\n",
                    "\n",
                    "url = \"https://raw.githubusercontent.com/mwaskom/seaborn-data/master/diamonds.csv\"\n",
                    "diamonds_pd = pd.read_csv(url)\n",
                    "diamonds_pd.to_csv(\"/home/jovyan/work/diamonds.csv\", index=False)\n",
                    "print(f\"Dataset downloaded: {len(diamonds_pd)} rows.\")\n"
                ]
                print("Updated SparkSession initialization with explicit configs.")

    with open(path, 'w', encoding='utf-8') as f:
        json.dump(nb, f, indent=1)

if __name__ == '__main__':
    fix_task3()
