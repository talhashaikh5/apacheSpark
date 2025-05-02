from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sqlDataframe").getOrCreate()

people = spark.read.option("header","true")\
    .option("inferSchema","true")\
    .csv("./data/fakefriends-header.csv")

# print("Here is infered schema")
# people.printSchema()

# print("Select name column")
# people.select("name","age").show()

# print("Filter data")
# people.filter((people.age > 60) & (people.age <70)).select(people.name, people.age +10).show()


people.createOrReplaceTempView("people")

# thirteen = spark.sql(
#     """
#     SELECT * FROM people where age > 13
# """
# )
# thirteen.show()

averageFriends = spark.sql(
    """
        SELECT
            age,
            ROUND(SUM(friends)/COUNT(age)) as averageFriends
        FROM 
            people
        GROUP BY
            age
        ORDER BY
            averageFriends
    
    """
)

averageFriends.show(n=10000,truncate=False)

spark.stop()