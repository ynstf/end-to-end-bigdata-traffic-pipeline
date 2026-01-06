import random
import time
import json
import os
import sys
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Configuration
NUM_SENSORS = 10
ZONES = ["Downtown", "Suburb", "Industrial", "Residential"]
ROAD_TYPES = ["Highway", "Avenue", "Street", "Boulevard"]
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "traffic-events")

# Initialize sensors
sensors = []
for i in range(NUM_SENSORS):
    sensors.append({
        "sensor_id": f"sensor_{i:03d}",
        "road_id": f"road_{random.randint(1, 100):03d}",
        "road_type": random.choice(ROAD_TYPES),
        "zone": random.choice(ZONES)
    })

def create_producer():
    """
    Tries to establish a connection to Kafka with retries.
    """
    retries = 0
    while retries < 60:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(f"Connected to Kafka at {KAFKA_BROKER}")
            return producer
        except NoBrokersAvailable:
            print(f"Waiting for Kafka... (Attempt {retries+1}/10)")
            time.sleep(5)
            retries += 1
    raise Exception("Could not connect to Kafka")

def generate_event(sensor):
    """
    Generates a single traffic event for a given sensor.
    """
    current_time = datetime.now(timezone.utc)
    hour = current_time.hour
    
    # Simulate variable traffic based on time of day (more traffic during rush hours)
    is_rush_hour = (7 <= hour <= 9) or (17 <= hour <= 19)
    
    if is_rush_hour:
        vehicle_count = random.randint(20, 100)
        average_speed = random.uniform(10, 50)  # Slower speed during congestion
        occupancy_rate = random.uniform(50, 95)
    else:
        vehicle_count = random.randint(0, 30)
        average_speed = random.uniform(40, 90)  # Higher speed
        occupancy_rate = random.uniform(0, 40)
        
    event = {
        "sensor_id": sensor["sensor_id"],
        "road_id": sensor["road_id"],
        "road_type": sensor["road_type"],
        "zone": sensor["zone"],
        "vehicle_count": vehicle_count,
        "average_speed": round(average_speed, 1),
        "occupancy_rate": round(occupancy_rate, 1),
        "event_time": current_time.isoformat()
    }
    return event

def main():
    time.sleep(10)
    print(f"Starting Traffic Data Generator (Kafka Producer)...")
    print(f"Broker: {KAFKA_BROKER}, Topic: {TOPIC_NAME}")
    
    producer = create_producer()
    
    try:
        while True:
            for sensor in sensors:
                event = generate_event(sensor)
                producer.send(TOPIC_NAME, event)
                print(f"Sent: {event['sensor_id']} -> {event['vehicle_count']} vehicles")
                time.sleep(0.1) 
            time.sleep(1) 
            
    except KeyboardInterrupt:
        print("\nGenerator stopped by user.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
