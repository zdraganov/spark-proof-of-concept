import random
import os
import time
import json
from kafka import KafkaConsumer

kafka_host = os.environ['KAFKA_HOST']
kafka_port = os.environ['KAFKA_PORT']

raw_data_topic = os.environ['RAW_DATA_TOPIC']

def main():
    time.sleep(20)
    print("Consumer started!")

    consumer = KafkaConsumer(bootstrap_servers=f"{kafka_host}:{kafka_port}")
    consumer.subscribe([raw_data_topic])

    while True:
        for message in consumer:
            print(message.value)

        time.sleep(1)

main()
