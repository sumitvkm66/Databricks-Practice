# Databricks notebook source
df = spark.read.format('json')\
    .option('inferSchema','true')\
    .option('multiline','true')\
    .load('dbfs:/FileStore/resturant_json_data.json')

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.withColumn('restaurants',explode('restaurants'))\
    .select('*',
            'restaurants.restaurant.R.res_id',
            explode_outer('restaurants.restaurant.establishment_types').alias('establishment_type'),
            'restaurants.restaurant.name')\
    .select('res_id','establishment_type','name')\
    .show(truncate=False)

# COMMAND ----------

df.withColumn('restaurants',explode('restaurants')).count()
