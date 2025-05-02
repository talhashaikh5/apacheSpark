#  Import liberaries
from pyspark import SparkConf, SparkContext
import collections

# Configure Spark
conf = SparkConf().setMaster('local').setAppName("wordCount")
sc = SparkContext(conf = conf)

lines = sc.textFile("./data/book.txt")

wrodRDD = lines.flatMap(lambda x: x.split())
countWordRDD = wrodRDD.countByValue()

for word, count in countWordRDD.items():
    print(f"{word} -> {count}")

# collectedRDD = wrodRDD.collect()
# print(collectedRDD)
# # for key, value in collectedRDD.item():
# #     print(key,value)

