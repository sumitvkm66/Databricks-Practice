# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('leetcode').getOrCreate()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from datetime import datetime
schema = StructType({
    StructField('id',IntegerType()),
    StructField('visit_date', StringType()),
    StructField('people',IntegerType())
})

data = [
    (1, "2017-01-01", 10),
    (2, "2017-01-02", 109),
    (3, "2017-01-03", 150),
    (4, "2017-01-04", 99),
    (5, "2017-01-05", 145),
    (6, "2017-01-06", 1455),
    (7, "2017-01-07", 199),
    (8, "2017-01-09", 188)
]


# COMMAND ----------

stadium = spark.createDataFrame(data, schema)
stadium.show()

# COMMAND ----------

from pyspark.sql import Window

# COMMAND ----------

stadium_filter = stadium.filter(col('people')>=100).withColumn('rn',col('id')-row_number().over(Window.orderBy('id')))
valid_counts = stadium_filter.groupBy('rn').count().filter('count>=3').select('rn')

stadium_filter.join(valid_counts, 'rn','inner')\
    .select('id','visit_date','people')\
    .orderBy('visit_date')\
    .show()
