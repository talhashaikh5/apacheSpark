#  Import liberaries
from pyspark import SparkConf, SparkContext
import collections

# Configure Spark
conf = SparkConf().setMaster('local').setAppName("RatingHistogram")
sc = SparkContext(conf = conf)

# Load data
lines = sc.textFile("./ml-100k/u.data") # RDD object

# Transform data: get only rating value
ratings = lines.map(lambda x: x.split()[2])

# Call action on data
result = ratings.countByValue()

# Sort and print result
sortedResult = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResult.items():
    print(f"{key} , {value}")

sc.stop()
