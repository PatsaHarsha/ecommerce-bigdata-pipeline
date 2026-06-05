FROM python:3.9-slim

RUN apt-get update && apt-get install -y default-jre

WORKDIR /app

COPY bento_service.py .

RUN pip install --no-cache-dir bentoml mlflow boto3 pyspark

EXPOSE 3000

CMD ["bentoml", "serve", "bento_service:PurchasePredictor", "--host", "0.0.0.0", "--port", "3000"]
