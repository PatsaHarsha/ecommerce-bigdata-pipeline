
#!/usr/bin/env python3
# transform_transactions.py
# Standalone PySpark job — triggered by Airflow SparkSubmitOperator.
# Run manually: spark-submit transform_transactions.py --date 2026-04-27

import argparse   # argparse = parse command-line arguments passed by SparkSubmitOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg, round as _round

def main(date: str):
    spark = (
        SparkSession.builder
        .appName(f'TransformTransactions-{date}')   # name shown in Spark UI for this job
        .config('spark.hadoop.fs.s3a.endpoint',          'http://minio:9000')
        .config('spark.hadoop.fs.s3a.access.key',        'admin')
        .config('spark.hadoop.fs.s3a.secret.key',        'bigdata123')
        .config('spark.hadoop.fs.s3a.path.style.access', 'true')
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        .getOrCreate()
    )

    input_path = f's3a://warehouse/raw/transactions/{date}/data.csv'
    print(f'[INFO] Reading: {input_path}')

    df = spark.read.csv(input_path, header=True, inferSchema=True)
    print(f'[INFO] Total rows: {df.count()}')

    # Step 1: keep only successful transactions
    success_df = df.filter(col('status') == 'SUCCESS')

    # Step 2: group by country and compute summary statistics
    agg_df = (
        success_df
        .groupBy('country')
        .agg(
            count('txn_id').alias('txn_count'),           # how many transactions per country
            _round(_sum('amount'), 2).alias('total_amount'),   # total value processed
            _round(avg('amount'),  2).alias('avg_amount'),     # average transaction size
        )
        .orderBy('country')
    )

    # Write Parquet back to MinIO — partitioned by date for efficient future queries
    output_path = f's3a://warehouse/processed/transactions/date={date}/'
    print(f'[INFO] Writing: {output_path}')
    agg_df.write.mode('overwrite').parquet(output_path)   # overwrite = safe to re-run

    agg_df.show()
    print(f'[INFO] Done — {agg_df.count()} country rows written for date={date}')
    spark.stop()   # release resources when done

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', required=True, help='Processing date YYYY-MM-DD')
    args = parser.parse_args()
    main(args.date)
