# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('dl').getOrCreate()

# COMMAND ----------

from datetime import datetime
data = [
    (1, datetime.strptime("08/02/2022 10:00:00",'%m/%d/%Y %H:%M:%S'), "start"),
    (1, datetime.strptime("08/04/2022 10:00:00",'%m/%d/%Y %H:%M:%S'), "stop"),
    (2, datetime.strptime("08/17/2022 10:00:00",'%m/%d/%Y %H:%M:%S'), "start"),
    (2, datetime.strptime("08/24/2022 10:00:00",'%m/%d/%Y %H:%M:%S'), "stop")
]

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

schema = StructType([
    StructField("server_id", IntegerType(), True),
    StructField("status_time", TimestampType(), True),  # You can convert it to TimestampType later
    StructField("session_status", StringType(), True)
])



# COMMAND ----------

server_utilization = spark.createDataFrame(data, schema)
server_utilization.show()

# COMMAND ----------

server_utilization.select(
    sum(dayofmonth('status_time')*when(col('session_status')=='start',-1).otherwise(1)).alias('total_uptime_days')
).show()
