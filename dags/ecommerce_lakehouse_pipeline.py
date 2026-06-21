from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Batch path: feature engineering -> training -> validation.
# Assumes streaming ingestion (spark_streaming_consumer.py) has already
# populated s3a://ecommerce-lakehouse/events_raw/. That runs separately.

SPARK_MASTER = "spark://spark-master:7077"
JOBS = "/opt/airflow/jobs"

with DAG(
    dag_id="ecommerce_lakehouse_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["Z5008_Lab8"],
) as dag:

    feature_engineering = BashOperator(
        task_id="feature_engineering",
        bash_command=f"spark-submit --master {SPARK_MASTER} {JOBS}/spark_jobs/feature_engineering_job.py",
    )

    train_model = BashOperator(
        task_id="train_model",
        bash_command=f"spark-submit --master {SPARK_MASTER} {JOBS}/ml/train_model_job.py",
    )

    validate = BashOperator(
        task_id="validate_features",
        bash_command=f"spark-submit --master {SPARK_MASTER} {JOBS}/spark_jobs/validate_job.py",
    )

    feature_engineering >> train_model >> validate
