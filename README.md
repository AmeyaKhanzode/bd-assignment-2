# VM Setup
1. Broker
	- runs kafka and zookeper
```
apt update
apt install openjdk-11-jdk wget -y
wget https://downloads.apache.org/kafka/3.5.0/kafka_2.13-3.5.0.tgz
tar -xzf kafka_2.13-3.5.0.tgz
mv kafka_2.13-3.5.0 /opt/kafka
cd /opt/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties &
bin/kafka-server-start.sh config/server.properties &
```

2. Producer
	- publishes server data
```
apt update && apt install python3-pip -y
pip install kafka-python
python3 producer.py
```

3. Consumers
	- consumer cpu and stuff
```
apt update && apt install python3-pip openjdk-11-jdk -y
pip install kafka-python pyspark pandas
python3 consumer_cpu_mem.py
# or
python3 consumer_net_disk.py
```