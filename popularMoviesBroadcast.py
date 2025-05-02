from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
import codecs

def loadMovieNames():
    movieNames = {}

    with codecs.open("./ml-100k/u.item","r",encoding='ISO-8859-1',errors="ignore") as f:
        for line in f:
            fields = line.split('|')
            movieNames[fields[0]] = fields[1]
    return movieNames

spark = SparkSession.builder.appName("Movie name Broadcast").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

nameDict = spark.sparkContext.broadcast(loadMovieNames())

# Schema
schema = StructType(
    [
        StructField("userId",StringType(),True),
        StructField("movieId",StringType(),True),
        StructField("rating",IntegerType(),True),
        StructField("timestamp",TimestampType(),True)
    ]
)

# Load Data
df = spark.read\
        .option("sep","\t")\
        .schema(schema)\
        .csv("./ml-100k/u.data")
df.createOrReplaceTempView("df")

# show summary
df.describe().show()

mostRated = df\
            .groupBy(df.movieId)\
            .agg(func.count(df.rating).alias("totalRatings"))
mostRated = mostRated\
            .orderBy(mostRated.totalRatings,ascending=False)

def lookupName(movieId):
    return nameDict.value[movieId]
# Declare UDF
lookupNameUDF = func.udf(lookupName)
# Register UDF
spark.udf.register("lookupName",lookupName)

moviesWithName = mostRated.withColumn("movieName",lookupNameUDF(mostRated.movieId))

moviesWithName.show(5,truncate=False)

# UDF join in sql query
top5 = spark.sql(
    """
        SELECT
            movieId,
            COUNT(rating) as totalRatings,
            lookupName(movieId) as movieName
        FROM
            df
        GROUP BY
            1
        ORDER BY
            2 DESC
        LIMIT 5
        
    """
)

top5.show(5,truncate=False)

spark.stop()