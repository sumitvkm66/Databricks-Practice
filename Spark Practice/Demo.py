# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Sample data
data = [
    (1, "Alice", 30, 70000.0, "USA", "F"),
    (2, "Bob", 28, 68000.0, "INDIA", "M"),
    (3, "Charlie", 35, 80000.0, "UK", "M"),
    (4, "Diana", 26, 62000.0, "USA", "F"),
    (5, "Ethan", 32, 75000.0, "INDIA", "M")
]

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True),
    StructField("address", StringType(), True),
    StructField("gender", StringType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show it
df.show()


# COMMAND ----------

df.dropna(how='all',subset=['name']).show()
