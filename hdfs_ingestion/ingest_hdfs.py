from kafka import KafkaConsumer
from hdfs import InsecureClient
import json
import os
import time
import uuid
from datetime import datetime

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "traffic-events")
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL", "10")) # seconds

def get_hdfs_client():
    """Connects to HDFS with retries."""
    retries = 0
    while retries < 15:
        try:
            client = InsecureClient(HDFS_URL, user=HDFS_USER)
            # Try to list root to check connection
            client.list('/')
            print(f"Connected to HDFS at {HDFS_URL}")
            return client
        except Exception as e:
            print(f"Waiting for HDFS... (Attempt {retries+1}/15) Error: {e}")
            time.sleep(10)
            retries += 1
    raise Exception("Could not connect to HDFS")

def flush_buffer(client, buffer):
    if not buffer:
        return

    print(f"Flushing {len(buffer)} events to HDFS...")
    
    # Group events by date and zone
    groups = {}
    for event in buffer:
        try:
            dt = datetime.fromisoformat(event['event_time'])
            date_str = dt.strftime('%Y-%m-%d')
            zone = event.get('zone', 'unknown')
            
            key = (date_str, zone)
            if key not in groups:
                groups[key] = []
            groups[key].append(event)
        except Exception as e:
            print(f"Error processing event: {e}")

    # Write files
    for key, events in groups.items():
        date_str, zone = key
        # HDFS Path: /data/raw/traffic/date={date}/zone={zone}/
        hdfs_path = f"/data/raw/traffic/date={date_str}/zone={zone}"
        filename = f"events_{int(time.time())}_{uuid.uuid4().hex[:8]}.json"
        full_path = f"{hdfs_path}/{filename}"

        try:
            # Prepare content (newline delimited JSON)
            content = "\n".join([json.dumps(e) for e in events])
            
            # Ensure directory exists? Hdfs library might handle or we need to simple write.
            # InsecureClient write often handles parent dirs or we explicit list.
            # Let's simple write.
            with client.write(full_path, encoding='utf-8', overwrite=True) as writer:
                writer.write(content)
            
            print(f"Wrote {len(events)} events to {full_path}")
        except Exception as e:
            print(f"Failed to write to {full_path}: {e}")

def main():
    print("Starting HDFS Ingestion Service...")
    
    # Connect to HDFS
    hdfs_client = get_hdfs_client()
    
    # Connect to Kafka
    print("Connecting to Kafka...")
    retries = 0
    while retries < 15:
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='hdfs-ingest-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print(f"Connected to Kafka at {KAFKA_BROKER}")
            break
        except Exception as e:
            print(f"Waiting for Kafka... (Attempt {retries+1}/15) Error: {e}")
            time.sleep(10)
            retries += 1
    else:
        raise Exception("Could not connect to Kafka")
    
    buffer = []
    last_flush_time = time.time()
    
    print("Listening for messages...")
    try:
        for message in consumer:
            buffer.append(message.value)
            
            current_time = time.time()
            if len(buffer) >= BATCH_SIZE or (current_time - last_flush_time) >= FLUSH_INTERVAL:
                flush_buffer(hdfs_client, buffer)
                buffer = []
                last_flush_time = current_time
                
    except KeyboardInterrupt:
        print("Stopping ingestion...")
        flush_buffer(hdfs_client, buffer) # Flush remaining
        
if __name__ == "__main__":
    main()
