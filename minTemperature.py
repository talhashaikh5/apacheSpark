from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("minTemperarture")
sc = SparkContext(conf = conf)

lines = sc.textFile("./data/1800.csv")

def parseLine(x: str) -> tuple:
    data = x.split(",")
    stationID = data[0]
    entryType = data[2]
    temp = (float(data[3])-32)*5/9
    return (stationID,entryType,temp)

parseLineRDD = lines.map(parseLine)

minTempRDD = parseLineRDD.filter(lambda x: "TMAX" in x[1])
stationTempRDD = minTempRDD.map(lambda x: (x[0],x[2]))
resultRDD = stationTempRDD.reduceByKey(lambda x,y: min(x,y))

collectedRDD = resultRDD.collect()
for key, value in collectedRDD:
    print(f"{key} -> {value}")

sc.stop()