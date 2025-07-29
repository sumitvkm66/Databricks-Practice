# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('dl').getOrCreate()

# COMMAND ----------

schema = [
    "policy_holder_id",
    "case_id",
    "call_category",
    "call_date",
    "call_duration_secs"
]
from datetime import datetime

data = [
    [1, "f1d012f9-9d02-4966-a968-bf6c5bc9a9fe", "emergency assistance", datetime.strptime("2023-04-13T19:16:53Z", "%Y-%m-%dT%H:%M:%SZ"), 144],
    [1, "41ce8fb6-1ddd-4f50-ac31-07bfcce6aaab", "authorisation", datetime.strptime("2023-05-25T09:09:30Z", "%Y-%m-%dT%H:%M:%SZ"), 815],
    [2, "9b1af84b-eedb-4c21-9730-6f099cc2cc5e", "n/a", datetime.strptime("2023-01-26T01:21:27Z", "%Y-%m-%dT%H:%M:%SZ"), 992],
    [2, "8471a3d4-6fc7-4bb2-9fc7-4583e3638a9e", "emergency assistance", datetime.strptime("2023-03-09T10:58:54Z", "%Y-%m-%dT%H:%M:%SZ"), 128],
    [2, "38208fae-bad0-49bf-99aa-7842ba2e37bc", "benefits", datetime.strptime("2023-06-05T07:35:43Z", "%Y-%m-%dT%H:%M:%SZ"), 619]
]


# COMMAND ----------

callers = spark.createDataFrame(data, schema)
callers.show()

# COMMAND ----------

uncategorised_calls = callers.filter((col('call_category')=='n/a')|(col('call_category')==''))
total_calls = callers.count()
uncategorised_calls.select(round(count("*")/lit(total_calls)*100,1).alias("uncategorised_call_pct")).show()
