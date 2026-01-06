from kafka import KafkaConsumer
import json
import os
import sys

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "traffic-events")

def main():
    print(f"Starting Test Consumer...")
    print(f"Connecting to {KAFKA_BROKER}, Topic: {TOPIC_NAME}")

    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        for message in consumer:
            event = message.value
            print(f"Received: {event}")
            
    except KeyboardInterrupt:
        print("\nConsumer stopped.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
