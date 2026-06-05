# Real-Time E-Commerce User Behavior Analytics & Purchase Prediction System

## Roll Number Metadata
**Roll Number:** [Insert Roll Number]
**Name:** [Insert Name]
**Course:** [Insert Course Name]
**Semester:** [Insert Semester]

## System Architecture Overview
This project implements an end-to-end 8-layer architecture for processing streaming e-commerce events and predicting user purchases in real-time.

1. **Data Ingestion:** Streaming Kaggle 'E-commerce Behavior Data' through Apache Kafka.
2. **Data Storage:** Persisting raw event streams into MinIO (S3) using Delta Lake.
3. **Data Processing:** PySpark batch and Structured Streaming jobs loading data from MinIO.
4. **Feature Engineering:** Sessionizing users and calculating aggregations (e.g., view count, cart count, session duration).
5. **Model Training:** Training a Spark MLlib Random Forest classifier for purchase prediction.
6. **Experiment Tracking:** Logging hyperparameters, metrics, and models using MLflow.
7. **Model Serving:** (Next Step) Containerizing the MLflow model into a REST API via BentoML.
8. **Monitoring:** (Planned) System metrics via Prometheus and Grafana.

## Setup and Execution Steps

### 1. Prerequisites
- Docker and Docker Compose installed
- Python 3.9+ installed
- Java 11+ installed

### 2. Environment Configuration
Copy the sample environment file and configure your local credentials:
```bash
cp .env.example .env
```

### 3. Spin Up the Infrastructure
Start the Kafka, MinIO, Spark, MLflow, and Postgres containers:
```bash
docker-compose up -d
```

### 4. Running the Pipeline
- **Start Ingestion:** `python data/kaggle_producer.py`
- **Start Streaming Consumer:** `spark-submit spark_jobs/spark_streaming_consumer.py`
- **Feature Engineering:** `spark-submit spark_jobs/feature_engineering_job.py`
- **Model Training:** `spark-submit ml/train_model_job.py`

### 5. Accessing UIs
- **JupyterLab:** `http://localhost:8888`
- **MLflow:** `http://localhost:5000`
- **Kafka UI:** `http://localhost:8080`
- **MinIO:** `http://localhost:9001`
- **Spark Master:** `http://localhost:8081`

## AI Tools Declaration
In accordance with course policies, development assistants and AI tools were utilized during the creation of this project. These tools assisted in code generation, infrastructure containerization, PySpark script drafting, and MLflow integration to ensure a robust and well-documented architecture. All AI-generated code has been reviewed, tested, and integrated manually.
