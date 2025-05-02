from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("saprkSQL").getOrCreate()

def mapper(x: str) -> Row:
    field = x.split(",")
    id = int(field[0])
    name = str(field[1].encode("utf-8"))
    age = int(field[2])
    friends = int(field[3])
    return Row(
        id=id, name=name, age=age, friends=friends
    )

lines = spark.sparkContext.textFile("./data/fakefriends.csv")
people = lines.map(mapper)

# create schema
schemaPeople = spark.createDataFrame(people).cache()
# create temp table
schemaPeople.createOrReplaceTempView("people")


# call on query
teenagers = spark.sql(
    """
        SELECT
            *
        FROM 
            people
        WHERE
            age >= 13 or age <= 19 
    """
)
# teenagers.show()

# for teen in teenagers.collect():
#     print(teen)


# schemaPeople.filter(col("id") > 10).groupBy("age").count().orderBy("age").show()
schemaPeople.filter((col("age") >= 13) | (col("age") <= 19)).show()