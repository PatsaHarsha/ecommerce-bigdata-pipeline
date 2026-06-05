import nbformat

nb_path = r"/home/jovyan/work/A5_zda25m009.ipynb"
with open(nb_path, 'r', encoding='utf-8') as f:
    nb = nbformat.read(f, as_version=4)

cells_to_add = [
    nbformat.v4.new_markdown_cell("# Task 2: Performance Tuning Challenge\n\nIn this section, we create an intentionally slow query and optimize it step-by-step."),
    nbformat.v4.new_code_cell("""# 1. Setup Data: Create a Large Table (>100k rows) and a Small Table
from pyspark.sql.functions import rand, randn, round, col, expr

# Create a large 'Sales' DataFrame (~500,000 rows)
# We use rand() to generate random store_ids between 1 and 500
df_sales = spark.range(0, 500000).withColumn("store_id", (rand() * 500).cast("int") + 1) \\
    .withColumn("product_id", (rand() * 100).cast("int")) \\
    .withColumn("amount", round(rand() * 1000, 2))

# Create a smaller 'Stores' DataFrame (500 rows)
df_stores = spark.range(1, 501).withColumnRenamed("id", "store_id") \\
    .withColumn("region", expr("CASE WHEN store_id % 4 = 0 THEN 'North' WHEN store_id % 4 = 1 THEN 'South' WHEN store_id % 4 = 2 THEN 'East' ELSE 'West' END")) \\
    .withColumn("city", expr("concat('City_', cast((rand() * 50) as int))"))

df_sales.createOrReplaceTempView("sales")
df_stores.createOrReplaceTempView("stores")

print(f"Sales count: {df_sales.count()}")
print(f"Stores count: {df_stores.count()}")"""),
    
    nbformat.v4.new_markdown_cell("## Baseline Run: Intentionally Slow Query\n\n- **Shuffle Partitions:** 200 (Default, too high for our data size, causing many small tasks)\n- **AQE:** Disabled\n- **Broadcast Join:** Disabled (forcing SortMergeJoin)"),
    nbformat.v4.new_code_cell("""import time

# Disable optimizations for the baseline run
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1") # Disable automatic broadcast join
spark.conf.set("spark.sql.shuffle.partitions", "200") # Default is 200

def run_query():
    start_time = time.time()
    
    # Intentionally slow query: Join, Multi-level GroupBy, and OrderBy
    result = df_sales.join(df_stores, "store_id") \\
        .groupBy("region", "city", "product_id") \\
        .agg({"amount": "sum", "store_id": "count"}) \\
        .orderBy(col("region").desc(), col("sum(amount)").desc())
        
    result.write.format("noop").mode("overwrite").save() # Force action without writing to disk
    
    end_time = time.time()
    print(f"Query took {end_time - start_time:.2f} seconds")
    return result

print("--- Running Baseline ---")
baseline_df = run_query()"""),
    
    nbformat.v4.new_markdown_cell("## Optimization A: Tuning Shuffle Partitions\n\nReducing `spark.sql.shuffle.partitions` from 200 to 10. Since our data isn't massively huge, 200 partitions creates too much task overhead (tiny tasks). Lowering it reduces scheduling overhead and network I/O."),
    nbformat.v4.new_code_cell("""# Optimization A: Tune Shuffle Partitions
spark.conf.set("spark.sql.shuffle.partitions", "10")

print("--- Running Optimization A: Tuned Partitions ---")
opt_a_df = run_query()"""),
    
    nbformat.v4.new_markdown_cell("## Optimization B: Broadcast Join\n\nSince `df_stores` is very small (500 rows), we can broadcast it to all worker nodes. This completely eliminates the expensive shuffle phase required for a SortMergeJoin."),
    nbformat.v4.new_code_cell("""from pyspark.sql.functions import broadcast

print("--- Running Optimization B: Broadcast Join ---")
start_time = time.time()

# We explicitly use broadcast(df_stores)
result_b = df_sales.join(broadcast(df_stores), "store_id") \\
    .groupBy("region", "city", "product_id") \\
    .agg({"amount": "sum", "store_id": "count"}) \\
    .orderBy(col("region").desc(), col("sum(amount)").desc())
    
result_b.write.format("noop").mode("overwrite").save()

end_time = time.time()
print(f"Query took {end_time - start_time:.2f} seconds")"""),
    
    nbformat.v4.new_markdown_cell("## Optimization C: Enable Adaptive Query Execution (AQE)\n\nAQE dynamically optimizes the query plan at runtime. It can coalesce post-shuffle partitions, dynamically switch join strategies, and optimize skewed joins.\n\n*Note: We reset the broadcast threshold and shuffle partitions to let AQE do its magic.*"),
    nbformat.v4.new_code_cell("""# Reset to defaults to let AQE handle it
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760") # 10MB default

# Enable AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")

print("--- Running Optimization C: AQE Enabled ---")
opt_c_df = run_query()"""),

    nbformat.v4.new_markdown_cell("## Optimization D: Caching\n\nIf we are going to query `df_sales` multiple times, caching it in memory prevents Spark from re-evaluating the DataFrame from the source (or re-generating the random data) for every action."),
    nbformat.v4.new_code_cell("""print("--- Caching the DataFrame ---")
df_sales.cache()
df_sales.count() # Action to materialize the cache

print("--- Running Optimization D: Cached Data ---")
opt_d_df = run_query()

# Unpersist to free up memory when done
df_sales.unpersist()""")
]

nb.cells.extend(cells_to_add)

with open(nb_path, 'w', encoding='utf-8') as f:
    nbformat.write(nb, f)

print("Cells added successfully!")
