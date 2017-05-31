#example build based on http://spark.apache.org/docs/2.1.0/api/python/pyspark.streaming.html and https://gist.github.com/rmoff/eadf82da8a0cd506c6c4a19ebd18037e

from pyspark import SparkConf, SparkContext
from operator import add
import sys
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from kafka import KafkaClient


def main():

	#create spark context
	sc = SparkContext(appName="SparkStreamingWithKafka")

	# create spark streaming context from spark context(sc) , second parameter is for batch duration
	ssc = StreamingContext(sc,10)

	# create kafka direct stream from spark streaming context and generated messeges from producer
	kafka_stream = KafkaUtils.createStream(ssc, "localhost:2181", "metadata.broker.list", {raw_data_topic:1})

	parsed = kafka_stream.map(lambda v: json.loads(v[1]))

	#Count the number of instance of each profile view
	profile_views = parsed.map(lambda views: (views['profile.view'],1)).\
		reducedByKey(lambda x,y: x + y)

	profile_views.pprint()

	ssc.start()
	ssc.awaitTermination()

main()