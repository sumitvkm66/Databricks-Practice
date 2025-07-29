# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('leetcode').getOrCreate()

# COMMAND ----------

schema = ["product_id", "product_name", "description"]
data = [
    (1, "Widget A", "This is a sample product with SN1234-5678"),
    (2, "Widget B", "A product with serial SN9876-1234 in the description"),
    (3, "Widget C", "Product SN1234-56789 is available now"),
    (4, "Widget D", "No serial number here"),
    (5, "Widget E", "Check out SN4321-8765 in this description")
]


# COMMAND ----------

products = spark.createDataFrame(data, schema)
products.show()

# COMMAND ----------

products.filter(col('description').rlike(r'\bSN[0-9]{4}-[0-9]{4}\b')).show(truncate=False)
