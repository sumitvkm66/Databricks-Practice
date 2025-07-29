# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('dl').getOrCreate()

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

emails_data = [
    [125, 7771, "2022-06-14 00:00:00"],
    [236, 6950, "2022-07-01 00:00:00"],
    [433, 1052, "2022-07-09 00:00:00"],
    [450, 8963, "2022-08-02 00:00:00"],
    [555, 6633, "2022-08-09 00:00:00"],
    [499, 2500, "2022-08-08 00:00:00"]
]
email_schema = StructType([
    StructField("email_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("signup_date", StringType(), True)
])

texts_data = [
    [6878, 125, "Confirmed"],
    [6994, 236, "Confirmed"],
    [8950, 450, "Not Confirmed"],
    [6920, 236, "Not Confirmed"],
    [8966, 450, "Not Confirmed"],
    [8010, 499, "Not Confirmed"]
]

# Define schema using StructType
text_schema = StructType([
    StructField("text_id", IntegerType(), True),
    StructField("email_id", IntegerType(), True),
    StructField("signup_action", StringType(), True)
])



# COMMAND ----------

emails = spark.createDataFrame(emails_data, email_schema)
emails.show()

texts = spark.createDataFrame(texts_data,text_schema)
texts.show()

# COMMAND ----------

emails.alias('a').join(texts.alias('b'), on='email_id', how='inner')\
    .filter(col('signup_action')=='Confirmed')\
    .select(round(count('*')/emails.count(),2).alias('confirm_rate'))\
    .show()
