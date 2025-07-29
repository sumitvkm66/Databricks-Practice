# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/

# COMMAND ----------

flights_df = spark.read.format('csv')\
    .option('header','true')\
    .option('inferSchema','true')\
    .load('/FileStore/flights_data.csv')

flights_df.show()

# COMMAND ----------

flights_df.rdd.getNumPartitions()

# COMMAND ----------

partitioned_flight_df = flights_df.repartition(4)

# COMMAND ----------

partitioned_flight_df.show()

# COMMAND ----------

partitioned_flight_df.rdd.getNumPartitions()

# COMMAND ----------

partitioned_flight_df.withColumn('partition_id',spark_partition_id())\
    .groupBy('partition_id').count()\
    .show()

# COMMAND ----------

partitioned_df = flights_df.repartition(300,'ORIGIN_COUNTRY_NAME')

# COMMAND ----------

partitioned_df.show()

# COMMAND ----------

partitioned_df.withColumn('partition_id',spark_partition_id()).groupBy('partition_id').count().show()

# COMMAND ----------

n = flights_df.select('ORIGIN_COUNTRY_NAME').distinct().count()
partitioned_df = flights_df.repartition(n,'ORIGIN_COUNTRY_NAME')

# COMMAND ----------

partitioned_df.show(10)

# COMMAND ----------

partitioned_df.rdd.getNumPartitions()

# COMMAND ----------

partitioned_df.withColumn('partition_id',spark_partition_id()).groupBy('partition_id').count().show()

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled','true')

# COMMAND ----------

partitioned_df.rdd.getNumPartitions()

# COMMAND ----------

coalesce_df = partitioned_df.coalesce(2)

# COMMAND ----------

coalesce_df.count()

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id
partitioned_df.coalesce(3).withColumn('partition_id',spark_partition_id()).groupBy('partition_id').count().show()

# COMMAND ----------

partitioned_df.re   partition(3).withColumn('partition_id',spark_partition_id()).groupBy('partition_id').count().show()

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled','false')

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, floor

# Create Spark Session
spark = SparkSession.builder \
    .appName("JoinRepartitionExample1") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

# Simulate a large fact table with a skewed join key (many rows with key=1)
fact_data = [(1, i) for i in range(1000)] + [(i, i*10) for i in range(2, 100)]
fact_df = spark.createDataFrame(fact_data, ["key", "fact_value"])

# Simulate a small dimension table with unique keys
dim_data = [(i, f"name_{i}") for i in range(1, 100)]
dim_df = spark.createDataFrame(dim_data, ["key", "name"])

# print("\n=== Join WITHOUT Repartition ===")
# # fact_df.join(dim_df, on="key").explain(True)
# new_df = fact_df.join(dim_df, on="key")
# new_df.show()


# COMMAND ----------

print("\n=== Join WITH Repartition ===")
fact_df_repart = fact_df.repartition("key")
dim_df_repart = dim_df.repartition("name")
fact_df_repart.join(dim_df_repart, on="key").explain()
# fact_df_repart.join(dim_df_repart, on="key").explain(True)

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CoalesceExample") \
    .getOrCreate()

# Simulate a large dataset
data = [(i, f"customer_{i % 100}") for i in range(100000)]
df = spark.createDataFrame(data, ["id", "customer_name"])

# See current partitions
print("Initial partitions:", df.rdd.getNumPartitions())

# 🔥 Coalesce to reduce number of partitions (without shuffle)
df_coalesced = df.coalesce(4)

print("After coalesce:", df_coalesced.rdd.getNumPartitions())

# Write output (this will write 4 files)
df_coalesced.count()

