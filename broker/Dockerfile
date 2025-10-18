FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Set working directory
WORKDIR /opt

# Install dependencies
RUN apt update && apt install -y openjdk-11-jre-headless python3 python3-pip tar wget && \
    rm -rf kafka kafka_2.13-3.5.0 && \
    wget https://archive.apache.org/dist/kafka/3.5.0/kafka_2.13-3.5.0.tgz && \
    tar -xzf kafka_2.13-3.5.0.tgz && \
    mv kafka_2.13-3.5.0 kafka && \
    rm kafka_2.13-3.5.0.tgz && \
    pip install kafka-python

# Switch into Kafka directory
WORKDIR /opt/kafka

# Expose Kafka & Zookeeper ports
EXPOSE 9092 2181

# Default command (interactive shell)
CMD ["bash"]
