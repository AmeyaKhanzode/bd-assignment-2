import json
import csv
from kafka import KafkaConsumer

print("[INFO] Starting Consumer 2 - Network and Disk")

# Initialize Network consumer
consumer_net = KafkaConsumer(
    'topic-net',
    bootstrap_servers=['broker:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    consumer_timeout_ms=10000,
    api_version=(2, 5, 0)
)

# Initialize Disk consumer
consumer_disk = KafkaConsumer(
    'topic-disk',
    bootstrap_servers=['broker:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    consumer_timeout_ms=10000,
    api_version=(2, 5, 0)
)

# Consume Network data
net_data = []
print("[INFO] Consuming Network data...")
try:
    for message in consumer_net:
        data = message.value
        net_data.append(data)
        if len(net_data) % 10 == 0:
            print(f"[NET] Received {len(net_data)} records")
except Exception as e:
    print(f"[INFO] Network consumption ended: {e}")

print(f"[INFO] Total Network records: {len(net_data)}")

# Consume Disk data
disk_data = []
print("[INFO] Consuming Disk data...")
try:
    for message in consumer_disk:
        data = message.value
        disk_data.append(data)
        if len(disk_data) % 10 == 0:
            print(f"[DISK] Received {len(disk_data)} records")
except Exception as e:
    print(f"[INFO] Disk consumption ended: {e}")

print(f"[INFO] Total Disk records: {len(disk_data)}")

# Write to CSV files
with open('/app/net_data.csv', 'w', newline='') as f:
    if net_data:
        writer = csv.DictWriter(f, fieldnames=['ts', 'server_id', 'net_in', 'net_out'])
        writer.writeheader()
        writer.writerows(net_data)
        print(f"[SUCCESS] {len(net_data)} Network records saved to /app/net_data.csv")

with open('/app/disk_data.csv', 'w', newline='') as f:
    if disk_data:
        writer = csv.DictWriter(f, fieldnames=['ts', 'server_id', 'disk_io'])
        writer.writeheader()
        writer.writerows(disk_data)
        print(f"[SUCCESS] {len(disk_data)} Disk records saved to /app/disk_data.csv")

consumer_net.close()
consumer_disk.close()
print("[DONE] Consumer 2 completed")
