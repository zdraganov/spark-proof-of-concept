#example build based on http://spark.apache.org/docs/2.1.0/api/python/pyspark.streaming.html and https://gist.github.com/rmoff/eadf82da8a0cd506c6c4a19ebd18037e

from pyspark import SparkConf, SparkContext
from operator import add
import sys
import pyspark
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json



def main():

	#create spark context
	sc = SparkContext(appName="SparkStreamingWithKafka")

	# create spark streaming context from spark context(sc) , second parameter is for batch duration
	ssc = StreamingContext(sc,10)

	# create kafka direct stream from spark streaming context and generated messeges from producer
	kafka_stream = KafkaUtils.createStream(ssc, "zookeeper:2181", "spark-streaming-consumer", {'raw_data':1})

	# parsed = kafka_stream.map(lambda v: json.loads(v[1]))
	lines = kafka_stream.map(lambda x: x[1])
	counts = lines.flatMap(lambda line: line.split(" ")) \
		.map(lambda word: (word,1)) \
		.reduceByKey(lambda a, b: a+b)

	counts.pprint()
	# print(parsed.count())

	ssc.start()
	ssc.awaitTermination()

main()