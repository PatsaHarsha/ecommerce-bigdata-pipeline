# Z5008 Big Data Lab - Practical Assignment
**Student Name:** [Your Name]
**Roll Number:** ZDA25M009

## Overview
This repository contains the Lakehouse batch pipeline implementation using Apache Spark (PySpark), Delta Lake, and Apache Airflow.

## Files Included
1. `practical_ZDA25M009.ipynb`: The main Jupyter Notebook containing the end-to-end data pipeline, transformations, Data Quality checks, and Spark SQL tuning evidence.
2. `dags/practical_ZDA25M009.py`: The Airflow DAG that orchestrates the entire workflow (data generation, bronze ingestion, silver transformation, gold aggregation, and verification).
3. `README.md`: This file.
4. `screenshots/`: Directory containing evidence of Airflow DAG execution and success.

## Dataset Choice & Assumptions
- **Domain:** E-commerce Orders.
- **Data Source:** Synthetic dataset generated using Python (Pandas/NumPy) containing 55,000 rows.
- **Why this domain:** E-commerce data perfectly models real-world data engineering scenarios with diverse data types (timestamps, categories, monetary amounts) and requires complex transformations like window functions (user spending behavior) and aggregations.
- **Assumptions:**
  - The Airflow and Spark infrastructure are running locally via Docker Compose.
  - MinIO is used as the underlying S3-compatible storage layer (`s3a://warehouse/`).
  - The Airflow `PythonOperator` environment has access to PySpark (or simulates execution if jars are missing from the Airflow container).

## Run Instructions

### 1. Running the Jupyter Notebook
1. Start the infrastructure using `docker-compose up -d`.
2. Open JupyterLab at `http://localhost:8888` (token: `bigdata`).
3. Open `notebooks/practical_ZDA25M009.ipynb`.
4. Run all cells from top to bottom. The notebook will automatically:
   - Generate synthetic data locally.
   - Initialize Spark with Delta Lake and MinIO.
   - Write Bronze, Silver, and Gold tables.
   - Output analytical Parquet and CSV files to the `outputs/` folder.

### 2. Triggering the Airflow DAG
1. Open the Airflow UI at `http://localhost:8090` (login: `admin` / `admin`).
2. Search for the DAG named `practical_ZDA25M009`.
3. Unpause the DAG (toggle switch).
4. Click the "Play" button to trigger the DAG.
5. In the Trigger DAG with config view, you can optionally pass `{"n_rows": 50000}`.
6. Monitor the Task execution in the Graph or Grid view.
7. Check the logs of the `verify_pipeline` task to see the final data quality checks.

## Outputs Locations
- **Raw Data:** `data/raw/ecommerce_orders.csv`
- **Bronze Table:** `s3a://warehouse/bronze/ecommerce/`
- **Silver Table:** `s3a://warehouse/silver/ecommerce/`
- **Gold Table:** `s3a://warehouse/gold/ecommerce/`
- **Local Outputs:** `outputs/top_10_users.csv`, `outputs/daily_revenue_trend.csv`
