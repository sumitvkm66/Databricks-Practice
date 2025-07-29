# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('leetcode').getOrCreate()

# COMMAND ----------

schema = ["user_id", "activity_date", "activity_type", "activity_duration"]
data = [
    (1, "2023-01-01", "free_trial", 45),
    (1, "2023-01-02", "free_trial", 30),
    (1, "2023-01-05", "free_trial", 60),
    (1, "2023-01-10", "paid", 75),
    (1, "2023-01-12", "paid", 90),
    (1, "2023-01-15", "paid", 65),
    (2, "2023-02-01", "free_trial", 55),
    (2, "2023-02-03", "free_trial", 25),
    (2, "2023-02-07", "free_trial", 50),
    (2, "2023-02-10", "cancelled", 0),
    (3, "2023-03-05", "free_trial", 70),
    (3, "2023-03-06", "free_trial", 60),
    (3, "2023-03-08", "free_trial", 80),
    (3, "2023-03-12", "paid", 50),
    (3, "2023-03-15", "paid", 55),
    (3, "2023-03-20", "paid", 85),
    (4, "2023-04-01", "free_trial", 40),
    (4, "2023-04-03", "free_trial", 35),
    (4, "2023-04-05", "paid", 45),
    (4, "2023-04-07", "cancelled", 0)
]


# COMMAND ----------

userActivity = spark.createDataFrame(data, schema)
userActivity.show()

# COMMAND ----------

userActivity.groupBy('user_id').agg(
    round(avg(when(col('activity_type')=='free_trial', col('activity_duration').cast("float")).otherwise(None)),2).alias('trial_avg_duration'),
    round(avg(when(col('activity_type')=='paid', col('activity_duration').cast("float")).otherwise(None)),2).alias('paid_avg_duration')
)\
.filter(col('paid_avg_duration').isNotNull())\
.show()
