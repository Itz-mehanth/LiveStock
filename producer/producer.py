from kafka import KafkaProducer
import json, time, random

# Connect to Kafka broker
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "stock_prices"

while True:
    data = {
        "symbol": random.choice(["AAPL", "TSLA", "GOOGL", "AMZN"]),
        "price": round(random.uniform(100, 2000), 2),
        "timestamp": time.time()
    }
    producer.send(topic, value=data)
    print(f"Sent: {data}")
    time.sleep(1)
