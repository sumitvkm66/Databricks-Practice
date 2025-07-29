# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName('dl').getOrCreate()

# COMMAND ----------

data = [
    ["2023-01-31", "AAPL", 142.28, 142.70, 144.34, 144.29],
    ["2023-02-28", "AAPL", 146.83, 147.05, 149.08, 147.41],
    ["2023-03-31", "AAPL", 161.91, 162.44, 165.00, 164.90],
    ["2023-04-30", "AAPL", 167.88, 168.49, 169.85, 169.68],
    ["2023-05-31", "AAPL", 176.76, 177.33, 179.35, 177.25]
]
schema = StructType([
    StructField("date", StringType(), True),
    StructField("ticker", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True)
])

# COMMAND ----------

stock_prices = spark.createDataFrame(data, schema)
stock_prices.show()

# COMMAND ----------

stock_prices.groupBy('ticker').agg(
    max('open').alias('highest_open'),
    min('open').alias('lowest_open')
).alias('a')\
    .join(stock_prices.alias('b'), (col('a.highest_open')==col('b.open'))&(col('a.ticker')==col('b.ticker')), how='inner')\
    .join(stock_prices.alias('c'), (col('a.lowest_open')==col('c.open'))&(col('a.ticker')==col('c.ticker')), how='inner')\
    .select(col('a.ticker'), 
                date_format(col('b.date'),'MMM-yyyy').alias('highest_mth'), 
                col('a.highest_open'), 
                date_format(col('c.date'),'MMM-yyyy').alias('lowest_mth'), 
                col('a.lowest_open'))\
    .show()

# COMMAND ----------

.select(col('a.ticker'), 
                date_format(col('b.date'),'MMM-yyyy').alias('highest_mth'), 
                col('a.highest_open'), 
                date_format(col('c.date'),'MMM-yyyy').alias('lowest_mth'), 
                col('a.lowest_open'))\
