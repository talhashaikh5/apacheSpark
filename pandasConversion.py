from pyspark.sql import SparkSession
import os
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
import pandas as pd
import pyspark.pandas as ps 


spark = SparkSession\
        .builder\
        .appName("pandasConversion")\
        .config("spark.sql.ansi.enabled","false")\
        .config("spark.executorEnv.PYARROW_IGNORE_TIMEZONE","1")\
        .getOrCreate()

# 1. Create pandas df
print("1. Create pandas df")
df = pd.read_csv("./data/fakeFriends.csv",names=["id","name","age","friends"])
df = df[:5]
print("Pandas DF")
print(type(df))
print(df)

# 2. Convert Pandas DF to Spark DF
print("2. Convert Pandas DF to Spark DF")
sparkDf = spark.createDataFrame(df)

print("Spark DF")
print(type(sparkDf))
sparkDf.show()

# 3. Tranaformation on Spark SF
print("3. Tranaformation on Spark SF")
sparkfFilterDf = sparkDf.filter(sparkDf.age > 35)
sparkfFilterDf.show()

# 4. Comvert Spark DF to Pandas DF
print("4. Comvert Spark DF to Pandas DF")
pandasDf = sparkfFilterDf.toPandas()
print(type(pandasDf))
print(pandasDf)

# 5. use pandas on spark for scalable operation
print("# 5. use pandas on spark for scalable operation")
ps_df = ps.DataFrame(pandasDf)
ps_df["age"] = ps_df["age"]+10
print(type(ps_df))
print(ps_df) 


# 6. Convert Pandas on Spark DF to Spark DF
print("Convert Pandas on Spark DF to Spark DF")
sparkPsDf = ps_df.to_spark()
print(type(sparkPsDf))
sparkPsDf.show()

spark.stop()