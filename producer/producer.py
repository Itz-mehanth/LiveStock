import os
import json
import time
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- Configuration ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "stock_prices"
STOCK_SYMBOLS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META"
]

def create_kafka_producer(retries=10, delay=5):
    """Attempt to connect to Kafka with retries."""
    for i in range(retries):
        try:
            print(f"Attempt {i+1}/{retries}: Connecting to Kafka broker at {KAFKA_BROKER}...")
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version_auto_timeout_ms=10000, # Increase timeout for initial connection
                request_timeout_ms=20000
            )
            print("Successfully connected to Kafka.")
            return producer
        except NoBrokersAvailable:
            print(f" Kafka not available. Retrying in {delay} seconds...")
            time.sleep(delay)
    print("Could not connect to Kafka after several retries. Exiting.")
    return None

def main():
    """Main function to run the producer."""
    producer = create_kafka_producer()

    if not producer:
        return

    print(f"\n🚀 Starting to send data for {len(STOCK_SYMBOLS)} symbols to topic '{TOPIC}'...")
    while True:
        try:
            data = {
                "symbol": random.choice(STOCK_SYMBOLS),
                "price": round(random.uniform(50, 5000), 2),
                "timestamp": time.time()
            }
            future = producer.send(TOPIC, value=data)
            future.get(timeout=10) # Wait for confirmation to ensure message is sent
            print(f"Sent: {data}")
            time.sleep(random.uniform(0.5, 2.0)) # Add some jitter to the send interval
        except Exception as e:
            print(f"An error occurred while sending data: {e}")
            print("Attempting to reconnect...")
            producer.close()
            producer = create_kafka_producer()
            if not producer:
                break # Exit if reconnection fails

if __name__ == "__main__":
    main()

