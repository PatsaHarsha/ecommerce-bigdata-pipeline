from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
import mlflow
import mlflow.spark

def main():
    spark = SparkSession.builder \
        .appName("TrainModelJobMLflow") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "bigdata123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Set up MLflow
    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment("ecommerce_purchase_prediction")

    print("📊 Loading prepared features from MinIO Delta Lake...")
    df = spark.read.format("delta").load("s3a://ecommerce-lakehouse/features_prepared/")

    print("🧩 Vectorizing features...")
    feature_cols = ["view_count", "cart_count", "total_session_value", "session_duration_seconds"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    
    print("✂️ Splitting data into training (80%) and testing (20%) sets...")
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    num_trees = 20
    max_depth = 5

    with mlflow.start_run() as run:
        print(f"🚀 Started MLflow run: {run.info.run_id}")
        
        # Log hyperparameters
        mlflow.log_param("numTrees", num_trees)
        mlflow.log_param("maxDepth", max_depth)

        print("🤖 Initializing and training RandomForestClassifier Pipeline...")
        rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=num_trees, maxDepth=max_depth)
        pipeline = Pipeline(stages=[assembler, rf])
        model = pipeline.fit(train_df)

        print("📈 Evaluating model performance on test set...")
        predictions = model.transform(test_df)

        evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
        roc_auc = evaluator.evaluate(predictions)

        # Log metrics
        mlflow.log_metric("roc_auc", roc_auc)
        print("========================================")
        print(f"🎯 Final Model ROC-AUC Score: {roc_auc:.4f}")
        print("========================================")

        # Log model & register it
        print("📦 Logging model to MLflow registry...")
        mlflow.spark.log_model(model, "model", registered_model_name="PurchasePredictionModel")
        
        print(f"✅ Run {run.info.run_id} completely successfully.")

if __name__ == "__main__":
    main()
