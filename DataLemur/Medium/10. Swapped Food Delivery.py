# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('dl').getOrCreate()

# COMMAND ----------

# Data as list of lists (each inner list represents a row)
data = [
    [1, "Chow Mein"],
    [2, "Pizza"],
    [3, "Pad Thai"],
    [4, "Butter Chicken"],
    [5, "Eggrolls"],
    [6, "Burger"],
    [7, "Tandoori Chicken"],
    [8, "Sushi"],
    [9, "Tacos"],
    [10, "Ramen"],
    [11, "Burrito"],
    [12, "Lasagna"],
    [13, "Salad"],
    [14, "Steak"],
    [15, "Spaghetti"]
]

# Schema as list of tuples (column name and data type)
schema = ("order_id", "item")



# COMMAND ----------

orders = spark.createDataFrame(data, schema)
orders.show()

# COMMAND ----------

window_spec = Window.orderBy('order_id')
orders.select('order_id',
              when(col('order_id')%2==1,coalesce(lead('item').over(window_spec),'item'))\
             .otherwise(lag('item').over(window_spec)).alias('item')
              ).show()
