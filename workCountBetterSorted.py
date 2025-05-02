#  Import liberaries
from pyspark import SparkConf, SparkContext
import re

def normaliseWord(x: str) -> str:
    x = x.lower()
    return re.compile(r'\W+', re.UNICODE).split(x.lower())

# Configure Spark
conf = SparkConf().setMaster('local').setAppName("wordCount")
sc = SparkContext(conf = conf)

lines = sc.textFile("./data/book.txt")

wrodRDD = lines.flatMap(normaliseWord)

wordCountRDD = wrodRDD.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
wordCountSortedRDD = wordCountRDD.map(lambda x: (x[1],x[0])).sortByKey(ascending=True).map(lambda x: (x[1],x[0]))


# countWordRDD = wrodRDD.countByValue()

for word, count in wordCountSortedRDD.collect():
    print(f"{word} -> {count}")


