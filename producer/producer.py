import csv
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['broker:9092'],
    value_serializer=lambda line:json.dumps(line).encode('utf-8')
)

file_path = "dataset.csv"

i = 0
with open(file_path, "r") as file:
    for line in file:
        line = line.strip()
        print(f"line: {line}")
        if not line or line.startswith("ts"):
            continue
        if i == 0:
            i += 1
            continue

        ts, server_id, cpu_pct, mem_pct, net_in, net_out, disk_io = line.split(',')
        
        t_cpu = {
                'ts': ts,
                'server_id': server_id,
                'cpu_pct': float(cpu_pct)
            }

        t_mem = {
                'ts': ts,
                'server_id': server_id,
                'topic_mem': float(mem_pct)
            }

        t_net = {
                'ts': ts,
                'server_id': server_id,
                'net_in': float(net_in),
                'net_out': float(net_out)
            }

        t_disk_io = {
                'ts': ts,
                'server_id': server_id,
                'disk_io': float(disk_io)
            }

        producer.send('topic-cpu', t_cpu)
        producer.send('topic-mem', t_mem)
        producer.send('topic-net', t_net)
        producer.send('topic-disk', t_disk_io)

        print(f"[SENT] Sent metrics for server {server_id}")

producer.flush()
producer.close()
print("[SUCCESS] Data published successfully")
