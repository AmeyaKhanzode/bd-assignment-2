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

To create topics
```
bin/kafka-topics.sh --create --topic topic-cpu --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic topic-mem --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic topic-net --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic topic-disk --bootstrap-server localhost:9092
```

For demo
```
(in tmux)
cd /opt/kafka
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties

bin/kafka-server-start.sh -daemon config/server.properties
```

Echo the below to /opt/kafka/config/server.properties
```
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://broker:9092
zookeeper.connect=localhost:2181
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
pip install kafka-python pyspark 
python3 consumer_cpu_mem.py
# or
python3 consumer_net_disk.py
```

Zerotier setup:
```
curl -s https://install.zerotier.com | sudo bash (on each vm)
zerotier-cli join <network ID>
```

To demo the project:
- get the IP of the broker