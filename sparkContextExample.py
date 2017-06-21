#example build based on http://spark.apache.org/docs/2.1.0/api/python/pyspark.streaming.html and https://gist.github.com/rmoff/eadf82da8a0cd506c6c4a19ebd18037e

from pyspark import SparkConf, SparkContext
from operator import add
import sys
import pyspark
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import redis
from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers='kafka:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))

redisClient = redis.StrictRedis(host='redis' , port=6379)

def handler(message):
    records = message.collect()
    for record in records:
        producer.send('aggregated_data', str(record))

    print("New message generated !")

def toRedis(message):
	redisQueue = message.collect()
	for record in redisQueue:
		redisClient.set('key1' , record)

	print("Record set in redis base")

def main():

	#create spark context
	sc = SparkContext(appName="SparkStreamingWithKafka")

	# create spark streaming context from spark context(sc) , second parameter is for batch duration
	ssc = StreamingContext(sc,5)

	kvs = KafkaUtils.createDirectStream(ssc,['raw_data'] , {"metadata.broker.list":"kafka:9092"})
	# create kafka direct stream from spark streaming context and generated messeges from producer
	# kafka_stream = KafkaUtils.createStream(ssc, "zookeeper:2181", "spark-streaming-consumer", {'raw_data':1})

	# kafka_stream.saveAsTextFiles('/data/out.txt')

	#decode data into python dict
	parsed = kvs.map(lambda v: json.loads(v[1]))
	parsed.foreachRDD(toRedis)
	# parsed.pprint()
	
	#counting orders with revenue_counter = True	
	# true_orders = parsed.filter(lambda x: str(x['revenue_counted']) == 'True') \
	# 	.count()

	# #send message to consumer with orders where revenue_counted = True
	# true_status = parsed.filter(lambda x: str(x['revenue_counted']) == 'True') \
	# 	.foreachRDD(handler)
	
	# #calculate total revenue of orders with revenue_counter = True and send it throw consumer
	# true_status = parsed.filter(lambda x: str(x['revenue_counted']) == 'True') \
	# 	.map(lambda x: int(x['revenue'])) \
	# 	.reduce(lambda x,y: x + y) \
	# 	.foreachRDD(handler)

	
	# false_status = parsed.filter(lambda x: str(x['revenue_counted']) == 'False') \
	# 	.count()

	# false_status = parsed.filter(lambda x: str(x['revenue_counted']) == 'False') \
	# 	.foreachRDD(handler)

	# false_stats = parsed.filter(lambda x: str(x['revenue_counted']) == 'False') \
	# 	.map(lambda x: int(x['revenue'])) \
	# 	.reduce(lambda x,y: x + y) \
	# 	.foreachRDD(handler)

	# false_status = parsed.filter(lambda x: str(x['revenue_counter']) == 'False').repartition(1).saveAsTextFiles('/data/false_out.txt')

	ssc.start()
	ssc.awaitTermination()
	# ssc.stop()



main()
