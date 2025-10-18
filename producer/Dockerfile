FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

# Install Python + dependencies
RUN apt update && apt install -y python3 python3-pip && \
    pip install kafka-python pandas && \
    apt clean

# Copy your producer code
COPY . /app

CMD ["bash"]
