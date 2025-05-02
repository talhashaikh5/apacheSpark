from time import sleep
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("customerPurchase").getOrCreate()

schema = StructType(
    [
        StructField(name="stationId", dataType=StringType(), nullable=True),
        StructField(name="orderId", dataType=StringType(), nullable=True),
        StructField(name="amount", dataType=FloatType(), nullable=True)
    ]
)

df = spark.read.schema(schema).csv("./data/customer-orders.csv")

df.printSchema()

groupedDF = df\
        .groupBy(df.stationId)\
        .agg(
            func.round(func.sum(df.amount),2).alias("totalAmount"),
            func.count(df.orderId).alias("totalOrders")
        )
sortedDF = groupedDF.orderBy(groupedDF.totalOrders,groupedDF.totalAmount,ascending=False)

sortedDF.show()

result = sortedDF.collect()

for id, amount, orders in result:
    print(f"Customer {id} spent ${amount} in {orders} orders.")


sortedDF.describe().show()

df.describe().show()

spark.stop()

