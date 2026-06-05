import json
import os

def modify():
    path = 'notebooks/lab01_getting_started.ipynb'
    with open(path, 'r', encoding='utf-8') as f:
        nb = json.load(f)
    
    code = [
        "# ============================================================\n",
        "# EXERCISE 2: 3 Original DataFrame Operations on Employees Data\n",
        "# ============================================================\n",
        "\n",
        "from pyspark.sql import Window\n",
        "from pyspark.sql.functions import (\n",
        "    rank, dense_rank, col, udf, avg, count, round, sum as spark_sum\n",
        ")\n",
        "from pyspark.sql.types import StringType\n",
        "\n",
        "# ---- OPERATION 1: Window Function — Rank Employees by Salary Within Each Country ----\n",
        "print(\"=== Operation 1: Window Function — Salary Rank per Country ===\")\n",
        "window_spec = Window.partitionBy(\"country\").orderBy(col(\"salary\").desc())\n",
        "\n",
        "df_ranked = df.withColumn(\"salary_rank\", rank().over(window_spec)) \\\n",
        "              .withColumn(\"dense_salary_rank\", dense_rank().over(window_spec))\n",
        "\n",
        "df_ranked.select(\"name\", \"country\", \"salary\", \"salary_rank\") \\\n",
        "         .orderBy(\"country\", \"salary_rank\") \\\n",
        "         .show()\n",
        "print(\"-> Window functions compute per-group positions WITHOUT collapsing rows — very powerful!\\n\")\n",
        "\n",
        "\n",
        "# ---- OPERATION 2: UDF — Categorise Salaries ----\n",
        "print(\"=== Operation 2: UDF — Salary Category ===\")\n",
        "\n",
        "@udf(StringType())\n",
        "def salary_category(salary):\n",
        "    \"\"\"Classifies employee salary into Junior / Mid-level / Senior tier.\"\"\"\n",
        "    if salary is None:\n",
        "        return \"Unknown\"\n",
        "    elif salary < 70000:\n",
        "        return \"Junior\"\n",
        "    elif salary < 85000:\n",
        "        return \"Mid-level\"\n",
        "    else:\n",
        "        return \"Senior\"\n",
        "\n",
        "df_with_category = df.withColumn(\"salary_tier\", salary_category(col(\"salary\")))\n",
        "df_with_category.select(\"name\", \"salary\", \"salary_tier\").show()\n",
        "\n",
        "# Aggregate by tier\n",
        "df_with_category.groupBy(\"salary_tier\") \\\n",
        "    .agg(\n",
        "        count(\"*\").alias(\"num_employees\"),\n",
        "        round(avg(\"salary\"), 2).alias(\"avg_salary\")\n",
        "    ) \\\n",
        "    .orderBy(\"avg_salary\") \\\n",
        "    .show()\n",
        "print(\"-> UDFs let you apply arbitrary Python logic column-by-column in Spark!\\n\")\n",
        "\n",
        "\n",
        "# ---- OPERATION 3: Pivot Table — Country vs Salary Tier ----\n",
        "print(\"=== Operation 3: Pivot Table — Employees per Salary Tier by Country ===\")\n",
        "pivot_df = df_with_category.groupBy(\"country\") \\\n",
        "    .pivot(\"salary_tier\", [\"Junior\", \"Mid-level\", \"Senior\"]) \\\n",
        "    .agg(count(\"*\"))\n",
        "\n",
        "pivot_df.fillna(0).orderBy(\"country\").show()\n",
        "print(\"-> Pivot reshapes long data to wide format — great for cross-tabulations!\\n\")\n",
        "\n",
        "# ============================================================\n",
        "# ADDED ORIGINAL OPERATIONS (Task 2.4)\n",
        "# ============================================================\n",
        "\n",
        "# ---- OPERATION 4: Self-join — Find Employees in the Same City ----\n",
        "print(\"=== Operation 4: Self-join — Employees in the Same City ===\")\n",
        "emp1 = df.alias(\"emp1\")\n",
        "emp2 = df.alias(\"emp2\")\n",
        "\n",
        "same_city_df = emp1.join(emp2, \n",
        "    (col(\"emp1.city\") == col(\"emp2.city\")) & (col(\"emp1.name\") < col(\"emp2.name\"))\n",
        ").select(col(\"emp1.name\").alias(\"Employee_1\"), col(\"emp2.name\").alias(\"Employee_2\"), col(\"emp1.city\"))\n",
        "\n",
        "same_city_df.show()\n",
        "\n",
        "# ---- OPERATION 5: Window Function — Running Total Salary by Country ----\n",
        "print(\"=== Operation 5: Running Total Salary by Country ===\")\n",
        "rt_window = Window.partitionBy(\"country\").orderBy(\"salary\").rowsBetween(Window.unboundedPreceding, Window.currentRow)\n",
        "\n",
        "df_rt = df.withColumn(\"running_salary_total\", spark_sum(\"salary\").over(rt_window))\n",
        "df_rt.select(\"country\", \"name\", \"salary\", \"running_salary_total\")\\\n",
        "     .orderBy(\"country\", \"running_salary_total\")\\\n",
        "     .show()\n",
        "\n",
        "# ---- OPERATION 6: Filtering with Regex — Names starting with A, B, or C ----\n",
        "print(\"=== Operation 6: Regex Filter — Names starting with A, B, or C ===\")\n",
        "df_regex = df.filter(col(\"name\").rlike(\"^[ABC]\"))\n",
        "df_regex.select(\"name\", \"city\", \"country\").show()\n"
    ]
    
    # Target the cell with "EXERCISE 2"
    for cell in nb['cells']:
        if cell['cell_type'] == 'code':
            source_text = "".join(cell['source'])
            if "EXERCISE 2:" in source_text:
                cell['source'] = code
                print("Successfully modified Exercise 2 cell.")
                break
    
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(nb, f, indent=1)

if __name__ == '__main__':
    modify()
