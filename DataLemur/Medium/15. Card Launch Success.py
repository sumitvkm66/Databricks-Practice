# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('dl').getOrCreate()

# COMMAND ----------

data = [
    [1, 2021, "Chase Sapphire Reserve", 170000],
    [2, 2021, "Chase Sapphire Reserve", 175000],
    [3, 2021, "Chase Sapphire Reserve", 180000],
    [3, 2021, "Chase Freedom Flex", 65000],
    [4, 2021, "Chase Freedom Flex", 70000]
]
schema = ["issue_month", "issue_year", "card_name", "issued_amount"]


# COMMAND ----------

monthly_cards_issued = spark.createDataFrame(data, schema)
monthly_cards_issued.show()

# COMMAND ----------

from pyspark.sql import Window
window_spec = Window.partitionBy('card_name').orderBy('issue_year','issue_month')
monthly_cards_issued.withColumn('rn', row_number().over(window_spec))\
    .filter("rn==1")\
    .select('card_name','issued_amount')\
    .orderBy(col('issued_amount').desc())\
    .show(truncate=False)
