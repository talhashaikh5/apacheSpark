from pyspark.sql import SparkSession
from pyspark.sql import functions as func


spark = SparkSession.builder.appName("wordCount").getOrCreate()

inputDF = spark.read.text("./data/book.txt")

wordCountDF = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("words"))
wordsWithEmptyString = wordCountDF.filter(wordCountDF.words != "")
wordsLowerDF = wordsWithEmptyString.select(func.lower(wordsWithEmptyString.words).alias("words"))
wordsCountDF = wordsLowerDF.groupBy("words").agg(func.count("*").alias("wordCount")).orderBy("wordCount",ascending=False)

wordsCountDF.show()

spark.stop()