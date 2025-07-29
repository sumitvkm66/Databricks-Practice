# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('datalemur').getOrCreate()

# COMMAND ----------

data =[
    (1, "Emma Thompson", 3800, 1, 6),
    (2, "Daniel Rodriguez", 2230, 1, 7),
    (3, "Olivia Smith", 7000, 1, 8),
    (4, "Noah Johnson", 6800, 2, 9),
    (5, "Sophia Martinez", 1750, 1, 11),
    (6, "Liam Brown", 13000, 3, None),
    (7, "Ava Garcia", 12500, 3, None),
    (8, "William Davis", 6800, 2, None),
    (9, "Isabella Wilson", 11000, 3, None),
    (10, "James Anderson", 4000, 1, 11),
    (11, "Mia Taylor", 10800, 3, None),
    (12, "Benjamin Hernandez", 9500, 3, 8),
    (13, "Charlotte Miller", 7000, 2, 6),
    (14, "Logan Moore", 8000, 2, 6),
    (15, "Amelia Lee", 4000, 1, 7)
]

schema = ("employee_id", "name", "salary", "department_id", "manager_id")

# COMMAND ----------

employee = spark.createDataFrame(data, schema)
employee.show()

# COMMAND ----------

from pyspark.sql import Window

# COMMAND ----------

employee.withColumn('rank', rank().over(Window.orderBy(col('salary').desc())))\
    .filter(col('rank')==2)\
    .select('salary')\
    .dropDuplicates()\
    .show()
