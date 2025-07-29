# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("leetcode").getOrCreate()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

data = [
    [1, 150, "2024-07-01"],
    [2, 200, "2024-07-01"],
    [3, 75, "2024-07-01"],
    [4, 300, "2024-07-02"],
    [5, 50, "2024-07-02"],
    [6, 120, "2024-07-03"]
]

schema = [ "transaction_id" , "amount" , "transaction_date" ]

# COMMAND ----------

transactions = spark.createDataFrame(data, schema)

# COMMAND ----------

transactions.show()

# COMMAND ----------

transactions.groupBy(col('transaction_date'))\
    .agg(
        sum(
            when(col('amount')%2==1,col('amount')).otherwise(0)
        ).alias('odd_sum'),
        sum(
            when(col('amount')%2==0,col('amount')).otherwise(0)
        ).alias('even_sum'),
    ).show()

# COMMAND ----------


