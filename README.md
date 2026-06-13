# Real-Time E-Commerce Big Data Pipeline & Machine Learning Lakehouse

**Author:** Patsa Harsha Sai  
**Roll Number:** zda25m009  
**Institution:** IIT Madras Zanzibar  

## 📖 Project Description
This repository contains a complete, end-to-end Real-Time Data Lakehouse architecture. It is designed to ingest, process, and model high-velocity e-commerce event streams to predict user purchase intent. The system is fully containerized across 14 interoperating services, bridging the gap between raw data engineering and production machine learning (MLOps).

By natively wrapping PySpark `PipelineModels` and routing distributed object storage through local Docker DNS, the architecture achieves a highly resilient, real-time REST API capable of serving predictions from raw JSON inputs.

## 📊 The Dataset
The pipeline processes a massive **20 million-row eCommerce behavior dataset**. 
* **Scope:** User session events (views, cart additions, purchases).
* **Features Extracted:** Dynamic sessionization metrics including `view_count`, `cart_count`, `session_duration_seconds`, and `total_session_value`.
* **Objective:** Predict whether a live user session will result in a successful purchase transaction.

### 📂 Data Requirements for Evaluation
To ensure the pipeline is reproducible, please place your local copies of the eCommerce dataset (`2019-Oct.csv` and `2019-Nov.csv`) inside the `/dataset` folder.

* **Expected Structure:**
  ```text
  ecommerce-bigdata-pipeline/
  ├── dataset/
  │   ├── 2019-Oct.csv
  │   └── 2019-Nov.csv
  ├── notebooks/
  └── ...
  ```

## 🏗️ System Architecture (8-Layer Stack)
1. **Ingestion:** Apache Kafka streams raw JSON events.
2. **Storage:** MinIO (S3-compatible) serves as the Delta Lake object store.
3. **Processing:** Apache Spark (Standalone Cluster with 1 Master, 2 Workers) handles both structured streaming and batch feature engineering.
4. **Machine Learning:** Distributed Random Forest trained via PySpark.
5. **Model Registry:** MLflow tracks experiments, hyperparameter tuning, and stores the serialized Pipeline weights.
6. **Orchestration:** Apache Airflow schedules and monitors DAG executions.
7. **Model Serving:** BentoML exposes a Layer-7 REST API (`/predict`) to serve real-time inferences.
8. **Observability:** Prometheus scrapes system telemetry, visualized through live Grafana dashboards.

### 🌐 Service URLs (after boot)
| Service | URL | Credentials |
|---|---|---|
| Spark Master UI | http://localhost:8080 | — |
| Kafka UI | http://localhost:8085 | — |
| MinIO Console | http://localhost:9001 | admin / bigdata123 |
| MLflow Tracking | http://localhost:5000 | — |
| Airflow Web UI | http://localhost:8090 | admin / admin |
| Grafana Dashboards | http://localhost:3001 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| **BentoML Predict API** | **http://localhost:3000/predict** | — |

## 📸 System Verification & Observability
System execution and monitoring screenshots have been captured and stored in the `/screenshots` directory to validate the live infrastructure:
* `spark_cluster_ui.png`: Active Spark Master and Worker node allocation.
* `minio_storage.png`: Local S3 object store configurations.
* `mlflow_registry.png`: Model tracking and version control interface.
* `airflow_dag.png`: Orchestration workflow graphs.
* *Grafana Observability:* Live metrics tracking cluster load (`process_cpu_seconds_total`) and API traffic.

## 🚀 How to Run the Infrastructure

**1. Staged Boot Sequence**
To prevent memory overloading on local machines, boot the core infrastructure first:
```bash
docker-compose up -d kafka minio postgres spark-master spark-worker-1 spark-worker-2 mlflow prometheus grafana
```

**2. Boot the Model API**
Once the core network is stable, build and launch the BentoML serving layer:
```bash
docker-compose up -d --build model-api
```
*(Allow 2-3 minutes for the API to pull Maven dependencies and deserialize the PySpark Pipeline from MinIO).*

**3. Live API Verification**
Send a test payload to the serving endpoint:
```python
import urllib.request, json

data = {
    "view_count": 5, 
    "cart_count": 1, 
    "total_session_value": 245.50, 
    "session_duration_seconds": 320
}

req = urllib.request.Request(
    'http://localhost:3000/predict', 
    data=json.dumps(data).encode(), 
    headers={'Content-Type': 'application/json'}
)
res = urllib.request.urlopen(req)
print(res.read().decode())
# Expected Output: {"predicted_purchase": 1.0}
```
