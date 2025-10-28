cd /opt/kafka

echo "listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://broker:9092
zookeeper.connect=localhost:2181" > /opt/kafka/config/server.properties

bin/zookeeper-server-start.sh config/zookeeper.properties &
bin/kafka-server-start.sh config/server.properties &

bin/kafka-topics.sh --create --topic topic-cpu --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic topic-mem --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic topic-net --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic topic-disk --bootstrap-server localhost:9092
