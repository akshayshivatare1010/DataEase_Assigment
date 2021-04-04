#!/usr/bin/env python
# coding: utf-8
# In[1]:


import findspark

findspark.init()


# In[4]:


import pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.sql("select 'spark' as hello ")

df.show()


# In[5]:


#Reading Data FIles :

df=spark.read.csv("Downloads\startup.csv",inferSchema=True,header=True)
df1=spark.read.parquet("Downloads\consumerInternet.parquet")

df3=df.unionAll(df1)

df3.printSchema()


# In[6]:


# How many startups are there in Pune City?
df3.createOrReplaceTempView("df3")

PuneStartups = spark.sql("SELECT COUNT(Startup_Name) FROM df3 WHERE City = 'Pune'")
PuneStartups.show()


# In[7]:


#How many startups in Pune got their Seed/ Angel Funding?

Startups = spark.sql("SELECT COUNT(Startup_Name) FROM df3 WHERE InvestmentnType like 'Seed%/%' AND City = 'Pune'")
Startups.show()


# In[32]:


#What is the total amount raised by startups in PuneCity? Hint - use regex_replace to get rid of null

PuneCityAmount = spark.sql("SELECT SUM(regexp_replace(Amount_in_USD, 'N/A', '00')) AS Amount FROM df3 WHERE City = 'Pune'")
PuneCityAmount.show()


# In[8]:


#What are the top 5 Industry_Vertical which has the highest number of startups in India?

TopFive = spark.sql("SELECT Industry_Vertical, COUNT(Startup_Name) FROM df3 GROUP BY Industry_Vertical ORDER BY COUNT(Startup_Name) DESC LIMIT 5")
TopFive.show()


# In[88]:


TopInvestor=spark.sql("SELECT Investors_Name  , COUNT(Amount_in_USD) FROM df3 GROUP BY Investors_Name ORDER BY COUNT(Amount_in_USD) DESC LIMIT 3 ")
TopInvestor.show()


# In[ ]:




