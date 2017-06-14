import os
import time
import json
import random
from kafka import KafkaProducer

kafka_host = os.environ['KAFKA_HOST']
kafka_port = os.environ['KAFKA_PORT']

raw_data_topic = os.environ['RAW_DATA_TOPIC']

def main():
    time.sleep(20)
    print("Producer started")

    producer = KafkaProducer(bootstrap_servers=f"{kafka_host}:{kafka_port}",
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    

    while True:
        producer.send(raw_data_topic, {'id':'{}'.format(random.randint(1 ,10000)) , 'revenue':'{}'.format(random.randint(1,10000)), 'revenue_counted':'{}'.format(random.choice([True, False]))})

        print("Message generated")
        time.sleep(1)

main()
