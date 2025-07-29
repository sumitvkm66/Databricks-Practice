# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('leetcode').getOrCreate()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

employee_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("departmentId", IntegerType(), True)
])

employee_data = [
    (1, "Joe", 85000, 1),
    (2, "Henry", 80000, 2),
    (3, "Sam", 60000, 2),
    (4, "Max", 90000, 1),
    (5, "Janet", 69000, 1),
    (6, "Randy", 85000, 1),
    (7, "Will", 70000, 1)
]
department_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])
department_data = [
    (1, "IT"),
    (2, "Sales")
]


# COMMAND ----------

employee = spark.createDataFrame(employee_data, employee_schema)
department = spark.createDataFrame(department_data, department_schema)
employee.show()
department.show()

# COMMAND ----------

from pyspark.sql import Window

# COMMAND ----------

employee.alias('e').join(department.alias('d'), col('e.departmentId')==col('d.id'), 'inner')\
    .withColumn('rank',dense_rank().over(Window.partitionBy('d.name').orderBy(col('salary').desc())))\
    .filter(col('rank')<=3)\
    .selectExpr('d.name as department','e.name as employee','salary')\
    .show()
