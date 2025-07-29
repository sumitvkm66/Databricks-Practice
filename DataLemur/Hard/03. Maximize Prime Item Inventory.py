# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('dl').getOrCreate()

# COMMAND ----------

inventory_schema = StructType([
    StructField("item_id", IntegerType(), True),
    StructField("item_type", StringType(), True),
    StructField("item_category", StringType(), True),
    StructField("square_footage", DoubleType(), True)
])

# Define data
inventory_data = [
    (1374, "prime_eligible", "mini refrigerator", 68.00),
    (4245, "not_prime", "standing lamp", 26.40),
    (2452, "prime_eligible", "television", 85.00),
    (3255, "not_prime", "side table", 22.60),
    (1672, "prime_eligible", "laptop", 8.50)
]

# COMMAND ----------

inventory = spark.createDataFrame(inventory_data, inventory_schema)
inventory.show()

# COMMAND ----------

count_sum = inventory.select(
    sum(when(col('item_type')=='prime_eligible',1)).alias('prime'),
    sum(when(col('item_type')!='prime_eligible',1)).alias('n_prime'),
    sum(when(col('item_type')=='prime_eligible',col('square_footage'))).alias('sum_p'),
    sum(when(col('item_type')!='prime_eligible',col('square_footage'))).alias('sum_np'),
)
count_sum.show()

# COMMAND ----------

prime = count_sum.select(lit('prime_eligible').alias('item_type'),
                 (floor(500000/col('sum_p'))*col('prime')).alias('item_count')
                 )

non_prime = count_sum.select(lit('not_prime'),
                 floor(
                     (500000- (floor(500000/col('sum_p'))*col('sum_p')))/col('sum_np')
                     )*col('n_prime')
                 )

prime.union(non_prime).show()

