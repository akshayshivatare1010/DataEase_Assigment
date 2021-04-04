import findspark

findspark.init()

import pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df=spark.read.csv("Downloads\startup.csv",inferSchema=True,header=True)
df1=spark.read.parquet("Downloads\consumerInternet.parquet")

df3=df.unionAll(df1)

df3.printSchema()

# Q4 What are the top 5 Industry_Vertical which has the highest number of startups in India?

TopFive = spark.sql("SELECT Industry_Vertical, COUNT(Startup_Name) FROM df3 GROUP BY Industry_Vertical ORDER BY COUNT(Startup_Name) DESC LIMIT 5")
TopFive.show()
