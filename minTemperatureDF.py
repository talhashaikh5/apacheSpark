from time import sleep
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("minTemperature").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType(
    [
        StructField(name="stationID", dataType=StringType(), nullable=True),
        StructField(name="date", dataType=IntegerType(), nullable=True),
        StructField(name="metric", dataType=StringType(), nullable=True),
        StructField(name="value", dataType=FloatType(), nullable=True)
    ]
)

df = spark.read.schema(schema).csv("./data/1800.csv")

df.printSchema()

filteredDF = df.filter(df.metric == "TMIN").orderBy("value")

# filteredDF.show()

resultDF = filteredDF\
            .select(filteredDF.stationID, filteredDF.value)\
            .groupBy(filteredDF.stationID)\
            .agg(func.min(filteredDF.value).alias("minTemp"))

resultCelciusDF = resultDF.select(
    resultDF.stationID,
    func.round(resultDF.minTemp*0.1*(9/5)+32,2).alias("minTempCelcius")
)
            

result = resultCelciusDF.collect()

print(result,type(result))

spark.stop()