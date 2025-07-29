# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName('dl').getOrCreate()

# COMMAND ----------

measurements_data = [
    [131233, 1109.51, "2022-07-10 09:00:00"],
    [135211, 1662.74, "2022-07-10 11:00:00"],
    [143562, 1124.50, "2022-07-11 13:15:00"],
    [346462, 1234.14, "2022-07-11 15:00:00"],
    [124245, 1252.62, "2022-07-11 16:45:00"],
    [523542, 1246.24, "2022-07-10 14:30:00"],
    [143251, 1246.56, "2022-07-11 18:00:00"],
    [141565, 1452.40, "2022-07-12 08:00:00"],
    [253622, 1244.30, "2022-07-12 14:00:00"],
    [353625, 1451.00, "2022-07-12 15:00:00"]
]

measurements_schema = StructType([
    StructField("measurement_id", IntegerType(), True),
    StructField("measurement_value", DoubleType(), True),
    StructField("measurement_time", StringType(), True)
])

# COMMAND ----------

measurements = spark.createDataFrame(measurements_data, measurements_schema)
measurements.show()

# COMMAND ----------

from pyspark.sql import Window

# COMMAND ----------

measurements.withColumn('rn',row_number().over(Window.partitionBy(col('measurement_time').cast('date')).orderBy('measurement_time')))\
    .groupBy(col('measurement_time').cast('date').alias('measurement_day')).agg(
        sum(expr("case when rn%2=1 then measurement_value else 0 end")).alias('odd_sum'),
        sum(expr("case when rn%2=0 then measurement_value else 0 end")).alias('even_sum')
        # sum(when(col('rn')%2==1,col('measurement_value')).otherwise(0)).alias('odd_sum'),
        # sum(when(col('rn')%2==0,col('measurement_value')).otherwise(0)).alias('even_sum')
    )\
    .show()
