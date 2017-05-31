import random
import os
import time
import json
import random
from kafka import KafkaProducer

kafka_host = os.environ['KAFKA_HOST']
kafka_port = os.environ['KAFKA_PORT']

raw_data_topic = os.environ['RAW_DATA_TOPIC']

MESSAGE_TYPES = [ 'profile.picture.like' , 'profile.view' , 'message.private']
START_INDEX = 1000
END_INDEX = 101000

def generate_random_message(start_index = START_INDEX , end_index = END_INDEX):
	return (random.randint(start_index,end_index), random.choice(MESSAGE_TYPES))

def main():
    time.sleep(20)
    print("Producer started")

    producer = KafkaProducer(bootstrap_servers=f"{kafka_host}:{kafka_port}",
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    while True:
        producer.send(raw_data_topic, generate_random_message)

        print("Message generated")
        time.sleep(1)

main()
