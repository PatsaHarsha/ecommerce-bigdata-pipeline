import json
import time
import random
from kafka import KafkaProducer

# 1. Setup the Producer
# Note: bootstrap_servers is 'localhost:9092' because the script 
# runs on your laptop, communicating with the Docker container.
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 2. Define our simulated E-commerce domain data [cite: 140, 142]
event_types = ['view', 'cart', 'purchase']

print("🚀 Starting Live Data Ingestion... (Press Ctrl+C to stop)")

try:
    while True:
        # Generate a realistic event record [cite: 149]
        data = {
            "event_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "event_type": random.choice(event_types),
            "product_id": random.randint(1000000, 9999999),
            "user_id": random.randint(100, 999),
            "price": round(random.uniform(5.0, 500.0), 2)
        }
        
        # 3. Send to Kafka topic 'user_events'
        producer.send('user_events', data)
        print(f"📡 Sent: {data}")
        
        # 4. Wait 1 second (essential for demo visibility) 
        time.sleep(1) 

except KeyboardInterrupt:
    print("\nStopping the stream. Ready for the next demo phase!")
finally:
    producer.close()
