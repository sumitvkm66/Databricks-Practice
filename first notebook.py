# Databricks notebook source
display(dbutils.fs.ls('FileStore'))

# COMMAND ----------

file_path = 'dbfs:/FileStore/Financial_Sample.xlsx'

df = spark.read.format("com.crealytics.spark.excel")\
    .option("header",'true')\
    .load(file_path)

display(df)



# COMMAND ----------

df.select(["Units Sold"]).show()
