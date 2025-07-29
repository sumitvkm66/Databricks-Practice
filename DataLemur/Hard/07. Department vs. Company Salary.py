# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('dl').getOrCreate()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("department_id", IntegerType(), True),
    StructField("manager_id", IntegerType(), True)
])

data = [
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

employee = spark.createDataFrame(data, schema)
employee.show()


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

salary_schema = StructType([
    StructField("salary_id", IntegerType(), True),
    StructField("employee_id", IntegerType(), True),
    StructField("amount", IntegerType(), True),
    StructField("payment_date", StringType(), True)
])

salary_data = [
    (1, 1, 3800, "2024-01-31 00:00:00"),
    (2, 2, 2230, "2024-01-31 00:00:00"),
    (3, 3, 7000, "2024-01-31 00:00:00"),
    (4, 4, 6800, "2024-01-31 00:00:00"),
    (5, 5, 1750, "2024-01-31 00:00:00"),
    (6, 6, 13000, "2024-01-31 00:00:00"),
    (7, 7, 12500, "2024-01-31 00:00:00"),
    (8, 8, 6800, "2024-01-31 00:00:00"),
    (9, 9, 11000, "2024-01-31 00:00:00"),
    (10, 10, 4000, "2024-01-31 00:00:00"),
    (11, 11, 10800, "2024-01-31 00:00:00"),
    (12, 12, 9500, "2024-01-31 00:00:00"),
    (13, 13, 7000, "2024-01-31 00:00:00"),
    (14, 14, 8000, "2024-01-31 00:00:00"),
    (15, 15, 4000, "2024-01-31 00:00:00"),
    (16, 1, 3800, "2024-02-28 00:00:00"),
    (17, 2, 2230, "2024-02-28 00:00:00"),
    (18, 3, 7000, "2024-02-28 00:00:00"),
    (19, 4, 6800, "2024-02-28 00:00:00"),
    (20, 5, 1750, "2024-02-28 00:00:00"),
    (21, 6, 13000, "2024-02-28 00:00:00"),
    (22, 7, 12500, "2024-02-28 00:00:00"),
    (23, 8, 6800, "2024-02-28 00:00:00"),
    (24, 9, 11000, "2024-02-28 00:00:00"),
    (25, 10, 4000, "2024-02-28 00:00:00"),
    (26, 11, 10800, "2024-02-28 00:00:00"),
    (27, 12, 9500, "2024-02-28 00:00:00"),
    (28, 13, 7000, "2024-02-28 00:00:00"),
    (29, 14, 8000, "2024-02-28 00:00:00"),
    (30, 15, 4000, "2024-02-28 00:00:00"),
    (31, 1, 3800, "2024-03-31 00:00:00"),
    (32, 2, 2230, "2024-03-31 00:00:00"),
    (33, 3, 7000, "2024-03-31 00:00:00"),
    (34, 4, 6800, "2024-03-31 00:00:00"),
    (35, 5, 1750, "2024-03-31 00:00:00"),
    (36, 6, 13000, "2024-03-31 00:00:00"),
    (37, 7, 12500, "2024-03-31 00:00:00"),
    (38, 8, 6800, "2024-03-31 00:00:00"),
    (39, 9, 11000, "2024-03-31 00:00:00"),
    (40, 10, 4000, "2024-03-31 00:00:00"),
    (41, 11, 10800, "2024-03-31 00:00:00"),
    (42, 12, 9500, "2024-03-31 00:00:00"),
    (43, 13, 7000, "2024-03-31 00:00:00"),
    (44, 14, 8000, "2024-03-31 00:00:00"),
    (45, 15, 4000, "2024-03-31 00:00:00")
]

salary = spark.createDataFrame(data=salary_data, schema=salary_schema)
display(salary)

# COMMAND ----------

department = salary.join(employee, salary.employee_id==employee.employee_id, 'inner')\
    .filter(date_format(col('payment_date'),'yyyyMM')=='202403')\
    .groupBy('department_id',date_format('payment_date','MM-yyyy').alias('payment_date')).agg(avg('amount').alias('avg_sal'))

company = salary.select(avg('amount').alias('avg_sal'))

department.crossJoin(company).select(
    'department_id','payment_date',
    when(department.avg_sal<company.avg_sal, 'lower')
    .when(department.avg_sal>company.avg_sal, 'higher')
    .otherwise('same')
    .alias('comparison')
)\
.orderBy(expr("case department_id when 1 then 1 when 2 then 3 else 2 end"))\
.show(truncate=False)
