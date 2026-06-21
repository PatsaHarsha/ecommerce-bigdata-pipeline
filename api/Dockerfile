FROM python:3.9-slim

RUN apt-get update && apt-get install -y default-jre && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bento_service.py .

EXPOSE 3000

CMD ["bentoml", "serve", "bento_service:PurchasePredictor", "--host", "0.0.0.0", "--port", "3000"]
