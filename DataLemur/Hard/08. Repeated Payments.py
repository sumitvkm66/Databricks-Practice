# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("TransactionsExample").getOrCreate()

# Define schema
schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("merchant_id", IntegerType(), True),
    StructField("credit_card_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_timestamp", StringType(), True)  # We'll convert this to Timestamp later
])

# Create data
data = [
    (1, 101, 1, 100.0, "09/25/2022 12:00:00"),
    (2, 101, 1, 100.0, "09/25/2022 12:08:00"),
    (3, 101, 1, 100.0, "09/25/2022 12:28:00"),
    (4, 102, 2, 300.0, "09/25/2022 12:00:00"),
    (6, 102, 2, 400.0, "09/25/2022 14:00:00")
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Convert string to timestamp
from pyspark.sql.functions import to_timestamp
transactions = df.withColumn("transaction_timestamp", to_timestamp("transaction_timestamp", "MM/dd/yyyy HH:mm:ss"))

# Show the DataFrame
transactions.show(truncate=False)


# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

a = transactions.alias('a')
b = transactions.alias('b')

a.join(b, (a.merchant_id==b.merchant_id) & (a.credit_card_id==b.credit_card_id)&(a.amount==b.amount)&(col('a.transaction_timestamp')<col('b.transaction_timestamp')), 'inner')\
    .filter(expr('timestampdiff(minute, a.transaction_timestamp, b.transaction_timestamp)')<10)\
    .select(count('*').alias('payement_count'))\
    .show()

# COMMAND ----------

transactions.createOrReplaceTempView('transactions')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as payment_count
# MAGIC from transactions a
# MAGIC join transactions b on a.merchant_id = b.merchant_id
# MAGIC     and a.credit_card_id = b.credit_card_id
# MAGIC     and a.amount = b.amount
# MAGIC     and a.transaction_timestamp < b.transaction_timestamp
# MAGIC where timestampdiff(minute, a.transaction_timestamp, b.transaction_timestamp) < 10
