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
countWordRDD = wrodRDD.countByValue()

for word, count in countWordRDD.items():
    print(f"{word} -> {count}")


