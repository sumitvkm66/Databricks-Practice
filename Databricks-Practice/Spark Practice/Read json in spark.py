# Databricks notebook source
# MAGIC %fs
# MAGIC ls /FileStore/JSON

# COMMAND ----------

# MAGIC %fs 
# MAGIC rm dbfs:/FileStore/JSON/line_delimited_multiline.json

# COMMAND ----------

df = spark.read.format("json")\
    .option("header",'true')\
    .option("inferSchema",'true')\
    .load("dbfs:/FileStore/JSON/line_delimited.json")

df.show()

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/JSON/extra_fields.json")
df.show()

# COMMAND ----------

df = spark.read.format("json")\
    .option("header","true")\
    .option("inferSchema","true")\
    .option("mode","PERMISSIVE")\
    .load("dbfs:/FileStore/JSON/corrupted_file.json")
df.show(truncate=False)

# COMMAND ----------

df = spark.read.format("json")\
    .option("header",'true')\
    .option("inferSchema",'true')\
    .option("multiline","true")\
    .load("dbfs:/FileStore/JSON/line_delimited_multiline.json")

df.show()

# COMMAND ----------

df = spark.read.format("json")\
    .option("header",'true')\
    .option("inferSchema",'true')\
    .option("multiline",'true')\
    .load("dbfs:/FileStore/JSON/extra_fields_multiline.json")

df.show()

# COMMAND ----------

df = spark.read.format("json")\
    .option("header",'true')\
    .option("inferSchema",'true')\
    .option("multiline",'true')\
    .option("mode",'PERMISSIVE')\
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .load("dbfs:/FileStore/JSON/corrupted_file_multiline.json")

df.show()

# COMMAND ----------

df = spark.read.format("json")\
    .option("multiline",'true')\
    .load("dbfs:/FileStore/JSON/corrupted_file_multiline-3.json")
df.cache()

# COMMAND ----------

df.show()

# COMMAND ----------


