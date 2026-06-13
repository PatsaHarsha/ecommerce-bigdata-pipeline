import os
os.environ["HADOOP_HOME"] = r"H:\m.tech\infrastructure\infrastructure\winutils"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

import bentoml
import mlflow
import pandas as pd

# Configure environment for MLflow and MinIO S3 access
os.environ["MLFLOW_TRACKING_URI"] = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
os.environ["MLFLOW_S3_ENDPOINT_URL"] = os.environ.get("MLFLOW_S3_ENDPOINT_URL", "http://localhost:9000")
os.environ["AWS_ACCESS_KEY_ID"] = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ.get("AWS_SECRET_ACCESS_KEY", "bigdata123")

try:
    # Pull latest model from MLflow registry into BentoML model store
    bentoml.mlflow.import_model(
        "purchase_prediction_model", 
        model_uri="models:/PurchasePredictionModel/latest"
    )
except Exception as e:
    print(f"Warning: Model import failed or already exists. Details: {e}")

from pyspark.sql import SparkSession
spark = (
    SparkSession.builder
    .appName("BentoML-Serving")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
    .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MLFLOW_S3_ENDPOINT_URL", "http://localhost:9000"))
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", "admin"))
    .config("spark.hadoop.fs.s3a.secret.key", "bigdata123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)


@bentoml.service(
    name="ecommerce_purchase_predictor",
    traffic={"timeout": 60}
)
class PurchasePredictor:
    def __init__(self):
        # Load the PyFunc model directly
        self.model = bentoml.mlflow.load_model("purchase_prediction_model:latest")
        
    @bentoml.api
    def predict(self, view_count: float, cart_count: float, total_session_value: float, session_duration_seconds: float) -> dict:
        # Construct dataframe matching PySpark schema
        input_data = pd.DataFrame([{
            "view_count": float(view_count),
            "cart_count": float(cart_count),
            "total_session_value": float(total_session_value),
            "session_duration_seconds": float(session_duration_seconds)
        }])
        
        # Predict
        prediction = self.model.predict(input_data)
        
        # Extract binary value flexibly (handles various PyFunc return formats)
        if isinstance(prediction, pd.DataFrame) and 'prediction' in prediction.columns:
            res = prediction['prediction'].iloc[0]
        elif isinstance(prediction, pd.Series):
            res = prediction.iloc[0]
        elif isinstance(prediction, (list, tuple)) or hasattr(prediction, '__iter__'):
            res = prediction[0]
        else:
            res = prediction
            
        return {
            "predicted_purchase": int(res)
        }
