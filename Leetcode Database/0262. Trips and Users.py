# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# Initialize Spark
spark = SparkSession.builder.appName("TripsUsersData").getOrCreate()

# Schema for Users
users_schema = StructType([
    StructField("users_id", IntegerType(), True),
    StructField("banned", StringType(), True),
    StructField("role", StringType(), True)
])

# Data for Users
users_data = [
    (1, 'No', 'client'),
    (2, 'Yes', 'client'),
    (3, 'No', 'client'),
    (4, 'No', 'client'),
    (10, 'No', 'driver'),
    (11, 'No', 'driver'),
    (12, 'No', 'driver'),
    (13, 'No', 'driver')
]

# Create Users DataFrame
users = spark.createDataFrame(users_data, schema=users_schema)

# Schema for Trips
trips_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("client_id", IntegerType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("city_id", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("request_at", DateType(), True)
])

# Data for Trips
from datetime import date
trips_data = [
    (1, 1, 10, 1, 'completed', date(2013, 10, 1)),
    (2, 2, 11, 1, 'cancelled_by_driver', date(2013, 10, 1)),
    (3, 3, 12, 6, 'completed', date(2013, 10, 1)),
    (4, 4, 13, 6, 'cancelled_by_client', date(2013, 10, 1)),
    (5, 1, 10, 1, 'completed', date(2013, 10, 2)),
    (6, 2, 11, 6, 'completed', date(2013, 10, 2)),
    (7, 3, 12, 6, 'completed', date(2013, 10, 2)),
    (8, 2, 12, 12, 'completed', date(2013, 10, 3)),
    (9, 3, 10, 12, 'completed', date(2013, 10, 3)),
    (10, 4, 13, 12, 'cancelled_by_driver', date(2013, 10, 3))
]

# Create Trips DataFrame
trips = spark.createDataFrame(trips_data, schema=trips_schema)


# COMMAND ----------

users.show()
trips.show()

# COMMAND ----------

c = users.alias('c')
d = users.alias('d')
t = trips.alias('t')

unbanned_trips = t.join(c, (col('t.client_id') == col('c.users_id')) & (col('c.banned')=='No') , 'inner')\
    .join(d, (col('t.client_id') == col('d.users_id')) & (col('d.banned')=='No') , 'inner')


unbanned_trips.filter(col('request_at').between('2013-10-01', '2013-10-03'))\
    .groupBy(col('request_at').alias('Day')).agg(
        round(sum(when(col('status')!='completed',1).otherwise(0))/count("*"),2).alias('Cancellation Rate')
        )\
    .show()
