from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define the DAG precisely as named in the lab handout
with DAG(
    dag_id='ecommerce_lakehouse_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['Z5008_Lab8'],
) as dag:

    # Task 1: Ingest
    ingest = BashOperator(
        task_id='ingest_to_bronze',
        bash_command='echo "Extracting CSV to MinIO Bronze layer..."; sleep 2'
    )

    # Task 2: Transform
    transform = BashOperator(
        task_id='transform_to_silver',
        bash_command='echo "Cleaning and partitioning Parquet to Silver layer..."; sleep 2'
    )

    # Task 3: Aggregate
    aggregate = BashOperator(
        task_id='aggregate_to_gold',
        bash_command='echo "Running Spark SQL aggregations to Gold layer..."; sleep 2'
    )

    # Task 4: Validate
    validate = BashOperator(
        task_id='validate_pipeline',
        bash_command='echo "Validating pipeline consistency..."; sleep 2'
    )

    # Set the dependency execution order
    ingest >> transform >> aggregate >> validate
