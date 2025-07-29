# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Test").getOrCreate()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

schema = "nzsic06 string ,   Area string, year int, geo_count int, ec_count int, _corrupt_record string"

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header","true")\
    .schema(schema)\
    .load("/FileStore/geographic_units.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

schema = "id int, name string, age int,salary int, address string, nominee string, _corrupt_record string"

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header","true")\
    .schema(schema)\
    .option("mode","PERMISSIVE")\
    .load("dbfs:/FileStore/employee.csv")

# COMMAND ----------

df.show(truncate=False)

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header","true")\
    .schema(schema)\
    .option("badRecordsPath","dbfs:/FileStore/")\
    .load("dbfs:/FileStore/employee.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/20250605T083031/bad_records/part-00000-cece1e19-c2de-4111-a6d6-40497c18c80d

# COMMAND ----------

bad_df = spark.read.format("json")\
    .option("header","true")\
    .load("/FileStore/20250605T083031/bad_records/part-00000-cece1e19-c2de-4111-a6d6-40497c18c80d")

# COMMAND ----------

display(bad_df)
