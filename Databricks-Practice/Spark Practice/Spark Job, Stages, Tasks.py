# Databricks notebook source
spark.conf.set('spark.sql.adaptive.enabled','false') #so that it does not reduce partiitions in case of wide dependency transformations.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
# schema = StructType([
#     StructField('id',IntegerType()),
#     StructField('name',StringType()),
#     StructField('age',IntegerType()),
#     StructField('salary',IntegerType())
# ])

employee_df = spark.read.format('csv')\
    .option('header','true')\
    .option('inferSchema','true')\
    .load('dbfs:/FileStore/employee.csv')

employee_df = employee_df.repartition(2)
employee_df = employee_df.filter(col('age')>90000)\
    .select('id','name','age','salary')\
    .groupBy('name','age')\
    .count()

employee_df.write.format('noop').mode('overwrite').save()

# employee_df.show()

# employee_df.collect()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Set shuffle partitions to 200 (or your desired number)
spark.conf.set("spark.sql.adaptive.enabled", "false")  # Disable AQE to avoid stage/task optimization

# Read CSV
employee_df = spark.read.format('csv')\
    .option('header','true')\
    .load('dbfs:/FileStore/employee.csv')

print("Original partitions:", employee_df.rdd.getNumPartitions())

# Repartition to force a shuffle stage (2 tasks)
employee_df = employee_df.repartition(2)
print("After repartition(2):", employee_df.rdd.getNumPartitions())

# Apply filters and groupBy
result_df = employee_df.filter(col('age') > 0)\
    .select('id', 'name', 'age', 'salary')\
    .groupBy('age')\
    .count()

# Trigger full shuffle with 200 tasks by writing to a dummy format
result_df.write.format("noop").mode("overwrite").save()

