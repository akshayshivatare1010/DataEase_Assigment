import findspark

findspark.init()

import pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df=spark.read.csv("Downloads\startup.csv",inferSchema=True,header=True)
df1=spark.read.parquet("Downloads\consumerInternet.parquet")

df3=df.unionAll(df1)

df3.printSchema()

#Q1 How many startups are there in Pune City?
df3.createOrReplaceTempView("df3")

PuneStartups = spark.sql("SELECT COUNT(Startup_Name) FROM df3 WHERE City = 'Pune'")
PuneStartups.show()
