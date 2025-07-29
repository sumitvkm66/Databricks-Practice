# Databricks notebook source
# MAGIC %fs
# MAGIC ls /FileStore/

# COMMAND ----------

# MAGIC %fs
# MAGIC mv dbfs:/FileStore/2015_summary.csv /FileStore/flights_data.csv

# COMMAND ----------

flights_df = spark.read.format('csv')\
    .option("header","true")\
    .option('inferSchema','true')\
    .load('dbfs:/FileStore/flights_data.csv')

# COMMAND ----------

flights_df.show()

# COMMAND ----------

flights_df_repartition = flights_df.repartition(5)
us_flight_data = flights_df_repartition.filter("DEST_COUNTRY_NAME=='United States'")
us_india_data = us_flight_data.filter("ORIGIN_COUNTRY_NAME=='India'")
total_flight_india = us_india_data.groupBy('DEST_COUNTRY_NAME').sum('count')
total_flight_india.show()
