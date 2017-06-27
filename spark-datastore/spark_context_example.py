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

kafkaProducer = KafkaProducer(bootstrap_servers='kafka:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
redisClient = redis.StrictRedis(host='redis', port=6379)

totalOrdersKey = 'abs_totals:orders'
totalRevenueKey = 'abs_totals:revenue'

def initializeAbsoluteTotals():
	if !redisClient.get(totalOrdersKey)
		redisClient.set(totalOrdersKey, 0)

	if !redisClient.get(totalRevenueKey)
		redisClient.set(totalRevenueKey, 0.0)

initializeAbsoluteTotals()

def processCall(callJson):
	id = callJson['id']
	previousCall = redisClient.get(id)

	if previousCall:
		handleCallUpdate(json.loads(previousCall), callJson)
	else:
		handleCallCreate(callJson)

def handleCallUpdate(previousCall, newCall):
	if previousCall['revenue_counted']:
		decrementAbsoluteTotals(previousCall['revenue'])

	if newCall['revenue_counted']:
		incrementAbsoluteTotals(newCall['revenue'])

	writeInRedis(newCall['id'], newCall)

def handleCallCreate(newCall):
	if newCall['revenue_counted']:
		incrementAbsoluteTotals(newCall['revenue'])

	writeInRedis(newCall['id'], newCall)

def incrementAbsoluteTotals(revenue):
	redisClient.incr(totalOrdersKey, 1)
	redisClient.incr(totalOrdersKey, revenue)

def decrementAbsoluteTotals(revenue):
	redisClient.decr(totalOrdersKey, 1)
	redisClient.decr(totalOrdersKey, revenue)

def notifyTotalUpdate():
	totalOrders = redisClient.get(totalOrdersKey)
	totalRevenue = redisClient.get(totalRevenueKey)

	kafkaProducer.send('aggregated_data', { total_revenue: totalRevenue, total_orders: totalOrders})

def writeInRedis(id, callJson):
	redisClient.set(id, str(callJson))

def main():
	#create spark context
	sc = SparkContext(appName="SparkStreamingWithKafka")
	# create spark streaming context from spark context(sc) , second parameter is for batch duration
	ssc = StreamingContext(sc,1)
	# create kafka direct stream
	kvs = KafkaUtils.createDirectStream(ssc, ['raw_data'], {"metadata.broker.list":"kafka:9092"})
	# loop over RDDs and process each of them
	kvs.map(lambda v: json.loads(v[1])) \
	.foreachRDD(lambda rddRecord: processCall(rddRecord.collect()); notifyTotalUpdate())
	# start streaming context
	ssc.start()
	# await for termination of the streaming context
	ssc.awaitTermination()

main()
