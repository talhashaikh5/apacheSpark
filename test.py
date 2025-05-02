from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName("ReduceByKeyExample")
sc = SparkContext(conf=conf)

wordCountsRDD = sc.parallelize([("apple", 2), ("banana", 3), ("apple", 5), ("orange", 1), ("banana", 2)])

# Use reduceByKey to sum the counts for each word
totalCountsRDD = wordCountsRDD.reduceByKey(lambda a, b: a + b)

# Collect and print the results
results = totalCountsRDD.collect()
print(results)

sc.stop()