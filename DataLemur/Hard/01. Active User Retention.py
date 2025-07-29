# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('dl').getOrCreate()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("event_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_date", TimestampType(), True)
])


from datetime import datetime

data = [
    [445, 7765, "sign-in", datetime.strptime("05/31/2022 12:00:00", "%m/%d/%Y %H:%M:%S")],
    [742, 6458, "sign-in", datetime.strptime("06/03/2022 12:00:00", "%m/%d/%Y %H:%M:%S")],
    [445, 3634, "like",    datetime.strptime("06/05/2022 12:00:00", "%m/%d/%Y %H:%M:%S")],
    [742, 1374, "comment", datetime.strptime("06/05/2022 12:00:00", "%m/%d/%Y %H:%M:%S")],
    [648, 3124, "like",    datetime.strptime("06/18/2022 12:00:00", "%m/%d/%Y %H:%M:%S")]
]



# COMMAND ----------

user_actions = spark.createDataFrame(data, schema)
user_actions.show()

# COMMAND ----------

user_actions_ym = user_actions.withColumns({
    'year':year('event_date'),
    'month':month('event_date')
})
a = user_actions_ym.alias('a')
b = user_actions_ym.alias('b')
a.filter((col('year')==2022)&(col('month')==6))\
    .join(b, 
          (col('a.user_id')==col('b.user_id'))
          &(col('a.year')==col('b.year'))
          &(col('a.month')==(col('b.month')+1)),
          'inner')\
    .groupBy('a.month').agg(countDistinct('a.user_id').alias('monthly_active_users'))\
    .show()

# COMMAND ----------

user_actions.distinct()

# COMMAND ----------

user_actions.select(dayofmonth('event_date')).show()
