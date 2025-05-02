from pyspark import SparkConf, SparkContext
from typing import Tuple

conf = SparkConf().setMaster('local').setAppName('customerPurchase')
sc = SparkContext(conf = conf)

def parseLine(x: str) -> Tuple[str,str]:
    data = x.split(",")
    customerID = int(data[0])
    orderID = int(data[1])
    amount = float(data[2])
    return (customerID,amount)

lines = sc.textFile("./data/customer-orders.csv")

amountRDD = lines.map(parseLine)
customerPurchaseRDD = amountRDD.reduceByKey(lambda x, y: x+y).sortByKey()
customerPurchaseSortedRDD = customerPurchaseRDD.map(lambda x: (x[1],x[0])).sortByKey().map(lambda x: (x[1],round(x[0])))

for customer, amount in customerPurchaseSortedRDD.collect():
    print(f"{customer} -> {amount}")
