# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName('dl').getOrCreate()

# COMMAND ----------

call_data = [
    [1, 2, "2022-07-04 10:13:49"],
    [1, 5, "2022-08-21 23:54:56"],
    [5, 1, "2022-05-13 17:24:06"],
    [5, 6, "2022-03-18 12:11:49"]
]
call_schema = ["caller_id", "receiver_id", "call_time"]

user_data = [
    [1, "US", "Verizon", "+1-212-897-1964"],
    [2, "US", "Verizon", "+1-703-346-9529"],
    [3, "US", "Verizon", "+1-650-828-4774"],
    [4, "US", "Verizon", "+1-415-224-6663"],
    [5, "IN", "Vodafone", "+91 7503-907302"],
    [6, "IN", "Vodafone", "+91 2287-664895"]
]
user_schema = ["caller_id", "country_id", "network", "phone_number"]


# COMMAND ----------

phone_calls = spark.createDataFrame(call_data, call_schema)
phone_info = spark.createDataFrame(user_data, user_schema)
phone_calls.show()
phone_info.show()

# COMMAND ----------

from pyspark.sql.functions import count, round, lit

# COMMAND ----------

a = phone_calls.alias('a')
b = phone_info.select('caller_id','country_id').alias('b')
c = phone_info.select(col('caller_id').alias('receiver_id'),'country_id').alias('c')

total_phonecalls = phone_calls.count()

result = (
    a.join(b, 'caller_id', 'inner')\
    .withColumnRenamed('country_id','caller_country')\
    .join(c, 'receiver_id', 'inner')\
    .withColumnRenamed('country_id','receiver_country')\
    .filter(col('caller_country')!=col('receiver_country'))\
    .select(round(count('*')/lit(total_phonecalls)*100,1).alias('international_calls_pct'))
)

result.show()
