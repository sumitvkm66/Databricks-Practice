# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('dl').getOrCreate()

# COMMAND ----------

data = [
    [1, 500],
    [2, 1000],
    [3, 800]
]

schema = ["item_count", "order_occurrences"]


# COMMAND ----------

items_per_order = spark.createDataFrame(data, schema)
items_per_order.show()

# COMMAND ----------

max_order = items_per_order.select(max('order_occurrences').alias('max_order'))
items_per_order.alias('a').join(max_order.alias('b'), col('a.order_occurrences')==col('b.max_order'), how="inner")\
    .select(col('item_count').alias('mode'))\
    .orderBy('mode')\
    .show()
