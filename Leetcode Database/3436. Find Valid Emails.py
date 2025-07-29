# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('leetcode').getOrCreate()

# COMMAND ----------

data = [
    [1, "alice@example.com"],
    [2, "bob_at_example.com"],
    [3, "charlie@example.net"],
    [4, "david@domain.com"],
    [5, "eve@invalid"]
]

schema = ['user_id','email_id']


# COMMAND ----------

users = spark.createDataFrame(data,schema)
users.show()

# COMMAND ----------

users.filter(col('email_id').rlike('^[a-zA-Z][a-zA-Z0-9_]*\@\w+\.com$'))\
    .show()
