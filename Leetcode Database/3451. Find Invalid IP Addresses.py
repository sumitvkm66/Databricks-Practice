# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('leetcode').getOrCreate()

# COMMAND ----------

data = [
    [1, "192.168.1.1", 200],
    [2, "256.1.2.3", 404],
    [3, "192.168.001.1", 200],
    [4, "192.168.1.1", 200],
    [5, "192.168.1", 500],
    [6, "256.1.2.3", 404],
    [7, "192.168.001.1", 200]
]
# data = [
#     [1, "95.118.75.115", 502],
#     [2, "66.36.109.557", 200],
#     [3, "64.7.141.75", 200],
#     [4, "230.223.71.131", 500],
#     [5, "249.219.186.220", 502],
#     [6, "60.177.134.229", 403],
#     [7, "225.227.90", 500],  # This IP seems incomplete.
#     [8, "91.182.129.190", 301]
# ]

schema = ['log_id','ip','status_code']

# COMMAND ----------

logs = spark.createDataFrame(data, schema)
logs.show()

# COMMAND ----------

logs.withColumn('octets',split(col('ip'),'\.'))\
    .select(length(col('ip'))-length(regexp_replace(col('ip'),'\.','')))\
    .show()

# COMMAND ----------

logs.withColumn('octets',split(col('ip'),'\.'))\
    .filter(
    (col('ip').contains('.0')) |
    (length(col('ip'))- length(regexp_replace(col('ip'),'\.',''))!=3) |
    expr('octets[0] not between 0 and 255')
)\
.show()

# COMMAND ----------

pattern = r'^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]?|0)\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]?|0)$'
# logs.filter(~col('ip').rlike(pattern))\
#     .groupBy('ip').agg(count('*').alias('invalid_count'))\
#     .show()
logs.filter(~col('ip').rlike(pattern)).show()

# COMMAND ----------


