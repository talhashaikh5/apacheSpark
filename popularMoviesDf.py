from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

# Schema
schema = StructType(
    [
        StructField("userId",StringType(),True),
        StructField("movieId",StringType(),True),
        StructField("rating",IntegerType(),True),
        StructField("timestamp",TimestampType(),True)
    ]
)

# spark initialization
spark = SparkSession\
        .builder\
        .appName("popularMovies")\
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

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

mostRated.show(5)

top5 = spark.sql(
    """
        SELECT
            movieId,
            COUNT(rating) as totalRatings
        FROM
            df
        GROUP BY
            1
        ORDER BY
            2 DESC
        LIMIT 5
        
    """
)

top5.show()


spark.stop()