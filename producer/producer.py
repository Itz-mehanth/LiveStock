from kafka import KafkaProducer
import json, time, random
import os

# Get Kafka broker host from environment variable, fallback to localhost
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

# Connect to Kafka broker
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "stock_prices"

print(f"Connecting to Kafka broker at {KAFKA_BROKER}...")

while True:
    data = {
        "symbol": random.choice(["AAPL", "TSLA", "GOOGL", "AMZN"]),
        "price": round(random.uniform(100, 2000), 2),
        "timestamp": time.time()
    }
    producer.send(topic, value=data)
    print(f"Sent: {data}")
    time.sleep(1)
