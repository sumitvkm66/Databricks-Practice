# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('dl').getOrCreate()
from pyspark.sql.types import StructType, StructField, IntegerType


# COMMAND ----------

customer_data = [
    [1, 1, 1000],
    [2, 2, 2000],
    [3, 1, 1100],
    [4, 1, 1000],
    [7, 1, 1000],
    [7, 3, 4000],
    [6, 4, 2000],
    [1, 5, 1500],
    [2, 5, 2000],
    [4, 5, 2200],
    [7, 6, 5000],
    [1, 2, 2000]
]

customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("amount", IntegerType(), True)
])

products_data = [
    [1, "Analytics", "Azure Databricks"],
    [2, "Analytics", "Azure Stream Analytics"],
    [3, "Containers", "Azure Kubernetes Service"],
    [4, "Containers", "Azure Service Fabric"],
    [5, "Compute", "Virtual Machines"],
    [6, "Compute", "Azure Functions"]
]
products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_category", StringType(), True),
    StructField("product_name", StringType(), True)
])


# COMMAND ----------

customers = spark.createDataFrame(customer_data, customer_schema)
customers.show()

products = spark.createDataFrame(products_data, products_schema)
products.show()

# COMMAND ----------

total_categories = products.select('product_category').distinct().count()

customers.alias('c').join(products.alias('p'), on='product_id', how='inner')\
    .groupBy('customer_id').agg(
        countDistinct('product_category').alias('categoryCount')
    )\
    .filter(col('categoryCount')==total_categories)\
    .select('customer_id')\
    .show()

# COMMAND ----------


