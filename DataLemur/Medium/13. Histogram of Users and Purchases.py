# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('dl').getOrCreate()


# COMMAND ----------

schema = ("product_id", "user_id", "spend","transaction_date")

data = [
    [3673, 123, 68.90, "2022-07-08 12:00:00"],
    [9623, 123, 274.10, "2022-07-08 12:00:00"],
    [1467, 115, 19.90, "2022-07-08 12:00:00"],
    [2513, 159, 25.00, "2022-07-08 12:00:00"],
    [1452, 159, 74.50, "2022-07-10 12:00:00"]
]


# COMMAND ----------

transactions = spark.createDataFrame(data, schema)
transactions.show()

# COMMAND ----------

recent_transaction = transactions.groupBy('user_id').agg(min('transaction_date').alias('recent_date'))
transactions.alias('a').join(recent_transaction.alias('b'), (col('a.user_id')==col('b.user_id')) & (col('a.transaction_date')==col('b.recent_date')), how="inner")\
    .groupBy('a.transaction_date','b.user_id').agg(count('*').alias('purchase_count'))\
    .orderBy('transaction_date')\
    .show()
