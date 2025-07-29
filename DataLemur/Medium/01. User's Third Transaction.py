# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('datalemur').getOrCreate()

# COMMAND ----------

data = [
    (111, 100.50, "01/08/2022 12:00:00"),
    (111, 55.00, "01/10/2022 12:00:00"),
    (121, 36.00, "01/18/2022 12:00:00"),
    (145, 24.99, "01/26/2022 12:00:00"),
    (111, 89.60, "02/05/2022 12:00:00")
]

schema = ['user_id', 'spend', 'transaction_date']

# COMMAND ----------

transactions = spark.createDataFrame(data, schema)
transactions.show()

# COMMAND ----------

from pyspark.sql import Window

# COMMAND ----------

window_spec = Window.partitionBy('user_id').orderBy('transaction_date')
transactions.alias('a').withColumn('rn',row_number().over(window_spec))\
    .filter(col('rn')==3)\
    .select('a.user_id', 'spend', 'transaction_date')\
    .show()
