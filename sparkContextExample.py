#example build based on http://spark.apache.org/docs/2.1.0/api/python/pyspark.streaming.html and https://gist.github.com/rmoff/eadf82da8a0cd506c6c4a19ebd18037e

from pyspark import SparkConf, SparkContext
from operator import add
import sys
import pyspark
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from kafka import KafkaProducer

# producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# def handler(message):
# 	records = message.collect()
#     for record in records:
#         producer.send('spark.out', str(record))


def main():

	#create spark context
	sc = SparkContext(appName="SparkStreamingWithKafka")

	# create spark streaming context from spark context(sc) , second parameter is for batch duration
	ssc = StreamingContext(sc,10)

	# kvs = KafkaUtils.createDirectStream(ssc,['test_topic'] , {"metadata.broker.list":"localhost:9092"})
	# create kafka direct stream from spark streaming context and generated messeges from producer
	kafka_stream = KafkaUtils.createStream(ssc, "zookeeper:2181", "spark-streaming-consumer", {'raw_data':1})

	parsed = kafka_stream.map(lambda v: json.loads(v[1]))
	lines = parsed.filter(lambda x: str(x['revenue_counted']) == 'True') \
		.map(lambda x: int(x['revenue'])) \
		.reduce(lambda x,y: x + y)
	

	lines.pprint()

	ssc.start()
	ssc.awaitTermination()



main()