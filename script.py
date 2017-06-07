from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("myTestApp")
sc = SparkContext(conf=conf)

v1='Hi hi hi bye bye bye word count' 
rdd=sc.parallelize([v1])


wordcounts=rdd.flatMap(lambda l: l.split(' ')) \
        .map(lambda w:(w,1)) \
        .reduceByKey(lambda a,b:a+b) \
        .map(lambda (a,b):(b,a)) \
        .sortByKey(ascending=False)


output = wordcounts.collect()
wordcounts.saveAsTextFile("/data/test.txt")
print(rdd.first())
print(output)

for (count,word) in output:
    print("%s: %i" % (word,count))

# textFile = sc.textFile("/var/lib/docker/volumes/5203d5a2ce2ed8319c16d08bc4acd11e8286bdc4ab31085db41cd309b714ca5f/_data/test.txt")
# textFile.count()
# # counts = textFile.flatMap(lambda line: line.split(" ")) \
# #              .map(lambda word: (word, 1)) \
# #              .reduceByKey(lambda a, b: a + b)

# counts.saveAsTextFile("/data/test2.txt")