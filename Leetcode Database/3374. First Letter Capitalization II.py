# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("leetcode").getOrCreate()
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

data = [
    [1, "hello world of SQL"],
    [2, "the QUICK-brown fox"],
    [3, "modern-day DATA science"],
    [4, "web-based FRONT-end development"]
]

schema = StructType([
    StructField("content_id",IntegerType()),
    StructField("content_text",StringType())
])

# COMMAND ----------

user_content = spark.createDataFrame(data, schema)
user_content.show(truncate=False)

# COMMAND ----------

user_content.withColumn('converted_text',regexp_replace(initcap(regexp_replace('content_text','-',' - ')),' - ','-'))\
    .show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

user_content.withColumn('converted_text', regexp_replace(
    initcap(
        regexp_replace(lower(col('content_text')),'-',' - ')
        ),' - ','-')
                        )\
    .show(truncate=False)
