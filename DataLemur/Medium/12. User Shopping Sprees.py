# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('dl').getOrCreate()

# COMMAND ----------

data = [
    [1, 9.99, "2022-08-01 10:00:00"],
    [1, 55.00, "2022-08-17 10:00:00"],
    [2, 149.5, "2022-08-05 10:00:00"],
    [2, 4.89, "2022-08-06 10:00:00"],
    [2, 34.00, "2022-08-07 10:00:00"]
]
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType, StringType

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_date", StringType(), True)
])

# COMMAND ----------

transactions = spark.createDataFrame(data, schema)
transactions.show()

# COMMAND ----------

transactions.alias('a').join(transactions.alias('b'), (col('a.user_id')==col('b.user_id'))&(col("a.transaction_date")==date_add(col('b.transaction_date'),-1)), how='inner')\
    .join(transactions.alias('c'), (col('a.user_id')==col('c.user_id'))&(col("a.transaction_date")=da(col('c.transaction_date'),-2)), how='inner')\
    .select('a.user_id')\
    .distinct()\
    .show()
