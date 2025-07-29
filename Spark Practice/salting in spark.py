# Databricks notebook source
spark.conf.set('spark.sql.adaptive.enabled','false')
spark.conf.set('spark.sql.shuffle.partitions',3)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, floor, concat_ws

# Spark session
spark = SparkSession.builder.appName("SaltingExample").getOrCreate()

# Simulate skewed fact data (many rows for customer_id=1)
fact_data = [(1, 100), (1, 200), (1, 300)] * 100000 + [(2, 150), (3, 180)] * 10
fact_df = spark.createDataFrame(fact_data, ["customer_id", "sales"])

# Simulate small dimension data (unique customer_id values)
dim_data = [(1, "Alice")]*2 + [(2, "Bob")]*2 + [(3, "Charlie")]
dim_df = spark.createDataFrame(dim_data, ["customer_id", "name"])

fact_df.join(dim_df, 'customer_id', 'inner').count()


# COMMAND ----------

# 🔥 Add a salt key to the fact table (0 to 9) to break skew
salted_fact_df = fact_df.withColumn("salt", floor(rand() * 10))
salted_fact_df = salted_fact_df.withColumn("salted_customer_id", concat_ws("_", col("customer_id"), col("salt")))

# 🔥 Expand the dim table for all salt values (cross join with salt range)
salt_range = spark.range(0, 10).withColumnRenamed("id", "salt")
salted_dim_df = dim_df.crossJoin(salt_range)
salted_dim_df = salted_dim_df.withColumn("salted_customer_id", concat_ws("_", col("customer_id"), col("salt")))

# 🔁 Salted join
joined_df = salted_fact_df.join(salted_dim_df, on="salted_customer_id", how="inner")

# ✅ Final output (optional: drop salted keys)
joined_df.count()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, floor

# Create a Spark session
spark = SparkSession.builder.appName("RandExample").getOrCreate()

# Sample data
data = [(1,), (2,), (3,), (4,), (5,)]
df = spark.createDataFrame(data, ["id"])

# Add a column with random values
df_with_rand = df.withColumn("random_value", floor(rand()*10+1))

df_with_rand.show()


# COMMAND ----------

dim_df.crossJoin(spark.range(0,10).withColumnRenamed('id','salt'))\
    .withColumn('salt_id',concat_ws('_','customer_id','salt'))\
    .show()

# COMMAND ----------

spark.conf.get('spark.memory.storageFraction')
