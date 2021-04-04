import findspark

findspark.init()

import pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df=spark.read.csv("Downloads\startup.csv",inferSchema=True,header=True)
df1=spark.read.parquet("Downloads\consumerInternet.parquet")

df3=df.unionAll(df1)

df3.printSchema()

#Find the top Investor(by amount) of each year.
TopInvestor=spark.sql("SELECT Investors_Name  , COUNT(Amount_in_USD) FROM df3 GROUP BY Investors_Name ORDER BY COUNT(Amount_in_USD) DESC LIMIT 3 ")
TopInvestor.show()
