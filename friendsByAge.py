from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("friendsByAge")
sc =  SparkContext(conf = conf)

lines = sc.textFile("fakefriends.csv")

def getData(x: str) -> tuple:
    name = x.split(",")[1]
    age = int(x.split(",")[2])
    friends =  int(x.split(",")[3])
    return (age,friends)

rdd = lines.map(getData)

totalByAge = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
sortByAge = totalByAge.sortByKey()
averageByAge = sortByAge.mapValues(lambda x: x[0]/x[1])


collectedValues = averageByAge.collect()
for key, value in collectedValues:
    print(f"{key},{value}")
