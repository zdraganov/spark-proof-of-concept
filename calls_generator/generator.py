import random
import os
import time
import json
from kafka import KafkaProducer

kafka_host = os.environ['KAFKA_HOST']
kafka_port = os.environ['KAFKA_PORT']

generator_topic = os.environ['GENERATOR_TOPIC']

def main():
    print("Start")
    time.sleep(5)
    producer = KafkaProducer(bootstrap_servers=f"{kafka_host}:{kafka_port}",
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    while True:
        producer.send(generator_topic, {'foo': 'bar'})

        print("Message generated")
        time.sleep(1)

main()
