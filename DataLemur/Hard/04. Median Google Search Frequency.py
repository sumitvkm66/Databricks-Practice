# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName('dl').getOrCreate()

# COMMAND ----------

data = [(1, 2), (2, 2), (3, 3), (4, 1),(5,1)]
columns = StructType([
    StructField('searches',IntegerType()),
    StructField('num_users',IntegerType())
]
)
search_frequency = spark.createDataFrame(data, columns)
search_frequency.show()

# COMMAND ----------

search_df = search_frequency.select(explode(array_repeat(col('searches'),col('num_users'))).alias('searches'))
search_df.show()

# COMMAND ----------

from pyspark.sql import Window

# COMMAND ----------

n = search_df.count()
new_df = search_df.withColumn('rn',row_number().over(Window.orderBy('searches')))\
    .filter(col('rn').isin(n/2,n/2+1,(n+1)/2))

new_df.select(
    when(lit(n)%2==1,max(col('searches'))).otherwise(round(avg(col('searches')),1)).alias('median')
).show()


# COMMAND ----------


