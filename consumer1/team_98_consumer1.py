import json
import csv
from kafka import KafkaConsumer
import time

print("[INFO] Starting Consumer 1 - CPU and Memory")

# Initialize CPU consumer
consumer_cpu = KafkaConsumer(
    'topic-cpu',
    bootstrap_servers=['broker:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    consumer_timeout_ms=10000,
    api_version=(2, 5, 0)
)

# Initialize Memory consumer
consumer_mem = KafkaConsumer(
    'topic-mem',
    bootstrap_servers=['broker:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    consumer_timeout_ms=10000,
    api_version=(2, 5, 0)
)

# Consume CPU data
cpu_data = []
print("[INFO] Consuming CPU data...")
try:
    for message in consumer_cpu:
        data = message.value
        cpu_data.append(data)
        if len(cpu_data) % 10 == 0:
            print(f"[CPU] Received {len(cpu_data)} records")
except Exception as e:
    print(f"[INFO] CPU consumption ended: {e}")

print(f"[INFO] Total CPU records: {len(cpu_data)}")

# Consume Memory data
mem_data = []
print("[INFO] Consuming Memory data...")
try:
    for message in consumer_mem:
        data = message.value
        mem_data.append(data)
        if len(mem_data) % 10 == 0:
            print(f"[MEM] Received {len(mem_data)} records")
except Exception as e:
    print(f"[INFO] Memory consumption ended: {e}")

print(f"[INFO] Total Memory records: {len(mem_data)}")

# Write to CSV files
with open('/app/cpu_data.csv', 'w', newline='') as f:
    if cpu_data:
        writer = csv.DictWriter(f, fieldnames=['ts', 'server_id', 'cpu_pct'])
        writer.writeheader()
        writer.writerows(cpu_data)
        print(f"[SUCCESS] {len(cpu_data)} CPU records saved to /app/cpu_data.csv")

with open('/app/mem_data.csv', 'w', newline='') as f:
    if mem_data:
        writer = csv.DictWriter(f, fieldnames=['ts', 'server_id', 'mem_pct'])
        writer.writeheader()
        writer.writerows(mem_data)
        print(f"[SUCCESS] {len(mem_data)} Memory records saved to /app/mem_data.csv")

consumer_cpu.close()
consumer_mem.close()
print("[DONE] Consumer 1 completed")
