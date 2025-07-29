# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('datalemur').getOrCreate()

# COMMAND ----------

activities_data = [
    (7274, 123, "open", 4.50, "06/22/2022 12:00:00"),
    (2425, 123, "send", 3.50, "06/22/2022 12:00:00"),
    (1413, 456, "send", 5.67, "06/23/2022 12:00:00"),
    (2536, 456, "open", 3.00, "06/25/2022 12:00:00"),
    (8564, 456, "send", 8.24, "06/26/2022 12:00:00"),
    (5235, 789, "send", 6.24, "06/28/2022 12:00:00"),
    (4251, 123, "open", 1.25, "07/01/2022 12:00:00"),
    (1414, 789, "chat", 11.00, "06/25/2022 12:00:00"),
    (1314, 123, "chat", 3.15, "06/26/2022 12:00:00"),
    (1435, 789, "open", 5.25, "07/02/2022 12:00:00")
]

activities_schema = ["activity_id", "user_id", "activity_type", "time_spent", "activity_date"]

age_data = [
    (123, "31-35"),
    (456, "26-30"),
    (789, "21-25")
]

# Schema list
age_schema = ["user_id", "age_bucket"]


# COMMAND ----------

activities = spark.createDataFrame(activities_data, activities_schema)
age_group = spark.createDataFrame(age_data,age_schema)
activities.show()
age_group.show()

# COMMAND ----------

age_group.alias('ag').join(activities.alias('ac'), col('ag.user_id')==col('ac.user_id'), how='inner')\
    .groupBy('age_bucket').agg(
        sum(when(col('activity_type')=='open',col('time_spent')).otherwise(0)).alias('open'),
        sum(when(col('activity_type')=='send',col('time_spent')).otherwise(0)).alias('send')
    )\
    .withColumns({
        'send_perc': round(100*(col('send')/(col('send')+col('open'))),2),
        'open_perc': round(100*(col('open')/(col('send')+col('open'))),2)
    })\
    .select('age_bucket','send_perc','open_perc')\
    .show()
