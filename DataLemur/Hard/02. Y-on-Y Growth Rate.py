# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('dl').getOrCreate()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType

schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("spend", DoubleType(), True),
    StructField("transaction_date", TimestampType(), True)
])
from datetime import datetime

data = [
    (1341, 123424, 1500.60, datetime.strptime("12/31/2019 12:00:00", "%m/%d/%Y %H:%M:%S")),
    (1423, 123424, 1000.20, datetime.strptime("12/31/2020 12:00:00", "%m/%d/%Y %H:%M:%S")),
    (1623, 123424, 1246.44, datetime.strptime("12/31/2021 12:00:00", "%m/%d/%Y %H:%M:%S")),
    (1322, 123424, 2145.32, datetime.strptime("12/31/2022 12:00:00", "%m/%d/%Y %H:%M:%S")),
]


# COMMAND ----------

user_transactions = spark.createDataFrame(data, schema)
user_transactions.show()

# COMMAND ----------

from pyspark.sql import Window
window_spec = Window.partitionBy('product_id').orderBy('year')

# COMMAND ----------

df_year = user_transactions.withColumn('year', year('transaction_date'))
df_curr_total = df_year.groupBy('year','product_id').agg(sum('spend').alias('curr_year_spend'))
df_prev_total = df_curr_total.withColumn('prev_year_spend',lag('curr_year_spend').over(window_spec))
df_yoy_rate = df_prev_total.withColumn('yoy_rate',round((col('curr_year_spend')-col('prev_year_spend'))/col('prev_year_spend')*100,2))

df_yoy_rate.orderBy('product_id','year').show()

# COMMAND ----------


