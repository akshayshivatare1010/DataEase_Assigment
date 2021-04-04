import findspark

findspark.init()

import pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df=spark.read.csv("Downloads\startup.csv",inferSchema=True,header=True)
df1=spark.read.parquet("Downloads\consumerInternet.parquet")

df3=df.unionAll(df1)

df3.printSchema()

#Q2 How many startups in Pune got their Seed/ Angel Funding?

Startups = spark.sql("SELECT COUNT(Startup_Name) FROM df3 WHERE InvestmentnType like 'Seed%/%' AND City = 'Pune'")
Startups.show()
