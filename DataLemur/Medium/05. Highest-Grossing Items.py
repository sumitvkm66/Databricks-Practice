# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('dl').getOrCreate()

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

schema = ["category", "product", "user_id", "spend", "transaction_date"]
# schema = StructType([
#     StructField('category',StringType()),
#     StructField('product',StringType()),
#     StructField('user_id',IntegerType()),
#     StructField('spend',DoubleType()),
#     StructField('transaction_date',TimestampType())
# ])

data = [
    ("appliance", "washing machine", 123, 219.80, "2022-03-02 11:00:00"),
    ("electronics", "vacuum", 178, 152.00, "2022-04-05 10:00:00"),
    ("electronics", "wireless headset", 156, 249.90, "2022-07-08 10:00:00"),
    ("electronics", "vacuum", 145, 189.00, "2022-07-15 10:00:00"),
    ("electronics", "computer mouse", 195, 45.00, "2022-07-01 11:00:00"),
    ("appliance", "refrigerator", 165, 246.00, "2021-12-26 12:00:00"),
    ("appliance", "refrigerator", 123, 299.99, "2022-03-02 11:00:00"),
    ("appliance", "washing machine", 123, 220.00, "2022-07-27 04:00:00"),
    ("electronics", "vacuum", 156, 145.66, "2022-08-10 04:00:00"),
    ("electronics", "wireless headset", 145, 198.00, "2022-08-04 04:00:00"),
    ("electronics", "wireless headset", 215, 19.99, "2022-09-03 16:00:00"),
    ("appliance", "microwave", 169, 49.99, "2022-08-28 16:00:00"),
    ("appliance", "microwave", 101, 34.49, "2023-03-01 17:00:00"),
    ("electronics", "3.5mm headphone jack", 101, 7.99, "2022-10-07 16:00:00"),
    ("appliance", "microwave", 101, 64.95, "2023-07-08 16:00:00")
]




# COMMAND ----------

product_spend = spark.createDataFrame(data,schema)
product_spend.show()

# COMMAND ----------

from pyspark.sql import Window

# COMMAND ----------

product_spend.filter(year(col('transaction_date'))==2022)\
.groupBy('category','product').agg(round(sum('spend'),2).alias('total_spend'))\
.withColumn('rn',row_number().over(Window.partitionBy('category').orderBy(col('total_spend').desc())))\
.filter(col('rn')<=2)\
.show()
