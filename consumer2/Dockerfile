FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

RUN apt update && apt install -y python3 python3-pip openjdk-11-jre-headless && \
    pip install kafka-python pandas pyspark==3.5.0 && \
    apt clean

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64

COPY . /app

CMD ["bash"]
