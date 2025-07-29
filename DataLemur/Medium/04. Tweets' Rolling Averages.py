# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('dl').getOrCreate()

# COMMAND ----------

schema = ["user_id", "tweet_date", "tweet_count"]

data = [
    (111, "06/01/2022 00:00:00", 2),
    (111, "06/02/2022 00:00:00", 1),
    (111, "06/03/2022 00:00:00", 3),
    (111, "06/04/2022 00:00:00", 4),
    (111, "06/05/2022 00:00:00", 5),
    (111, "06/06/2022 00:00:00", 4),
    (111, "06/07/2022 00:00:00", 6),
    (199, "06/01/2022 00:00:00", 7),
    (199, "06/02/2022 00:00:00", 5),
    (199, "06/03/2022 00:00:00", 9),
    (199, "06/04/2022 00:00:00", 1),
    (199, "06/05/2022 00:00:00", 8),
    (199, "06/06/2022 00:00:00", 2),
    (199, "06/07/2022 00:00:00", 2),
    (254, "06/01/2022 00:00:00", 1),
    (254, "06/02/2022 00:00:00", 1),
    (254, "06/03/2022 00:00:00", 2),
    (254, "06/04/2022 00:00:00", 1),
    (254, "06/05/2022 00:00:00", 3),
    (254, "06/06/2022 00:00:00", 1),
    (254, "06/07/2022 00:00:00", 3)
]


# COMMAND ----------

tweets = spark.createDataFrame(data, schema)
tweets.show()

# COMMAND ----------

from pyspark.sql import Window

# COMMAND ----------

window_spec = Window.partitionBy('user_id').orderBy('tweet_date').rowsBetween(-2,0)
tweets.withColumn('rolling_avg_3d',round(avg(col('tweet_count')).over(window_spec),2)).show(truncate=False)
