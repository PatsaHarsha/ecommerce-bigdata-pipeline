import csv
import json
import time
from kafka import KafkaProducer

def stream_kaggle_data(file_path, topic_name):
    # Initialize Kafka Producer (connecting to your local Docker exposed port)
    producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print(f"Starting to stream data from {file_path} to topic '{topic_name}'...")
    
    try:
        with open(file_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                # Map exactly to the required Kaggle schema
                data = {
                    "event_time": row.get("event_time"),
                    "event_type": row.get("event_type"),
                    "product_id": int(row.get("product_id")) if row.get("product_id") else None,
                    "category_id": row.get("category_id"),
                    "user_id": int(row.get("user_id")) if row.get("user_id") else None,
                    "user_session": row.get("user_session"),
                    "price": float(row.get("price")) if row.get("price") else 0.0
                }
                
                # Send to the specified Kafka topic
                producer.send(topic_name, value=data)
                print(f"Sent: {data}")
                
                # Delay to simulate real-time streaming velocity
                time.sleep(0.1)
                
    except FileNotFoundError:
        print(f"Error: Could not find the dataset at {file_path}.")
        print("Please ensure the Kaggle CSV is downloaded and the path is correct.")
    except KeyboardInterrupt:
        print("\nStreaming stopped manually. Ready for the next phase!")
    finally:
        producer.flush()
        producer.close()
        print("Kafka Producer closed.")

if __name__ == "__main__":
    # Specify the path where you will place your Kaggle CSV file
    DATASET_PATH = "data/raw/2019-Oct.csv"  # Adjust this filename if yours differs
    KAFKA_TOPIC = "ecommerce_events"
    
    stream_kaggle_data(DATASET_PATH, KAFKA_TOPIC)
