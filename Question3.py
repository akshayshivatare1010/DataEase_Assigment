
import findspark

findspark.init()

import pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df=spark.read.csv("Downloads\startup.csv",inferSchema=True,header=True)
df1=spark.read.parquet("Downloads\consumerInternet.parquet")

df3=df.unionAll(df1)

df3.printSchema()
# Q3 What is the total amount raised by startups in PuneCity? Hint - use regex_replace to get rid of null

PuneCityAmount = spark.sql("SELECT SUM(regexp_replace(Amount_in_USD, 'N/A', '00')) AS Amount FROM df3 WHERE City = 'Pune'")
PuneCityAmount.show()
