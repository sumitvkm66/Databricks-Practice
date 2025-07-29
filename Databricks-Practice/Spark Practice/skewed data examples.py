# Databricks notebook source
spark.conf.set('spark.sql.adaptive.enabled','true')

# COMMAND ----------

ggGspark.conf.get('spark.sql.autoBroadcastJoinThreshold')

# COMMAND ----------

from pyspark.sql.functions import expr, col, rand, round, when
from pyspark.sql.types import *
import math

n_rows = 50_000_000  # Roughly 10GB uncompressed

sales_df = spark.range(0, n_rows).withColumnRenamed("id", "order_id") \
    .withColumn("customer_id", expr("CAST(rand() * 100000 AS INT)")) \
    .withColumn("product_id", when(rand() < 0.7, 1)   # 70% of rows will have product_id = 1 (skewed)
                                .when(rand() < 0.9, 2)
                                .otherwise((rand() * 1000).cast("int"))) \
    .withColumn("quantity", expr("CAST(rand() * 10 + 1 AS INT)")) \
    .withColumn("price", round(rand() * 100 + 1, 2)) \
    .withColumn("discount", round(rand() * 0.2, 2)) \
    .withColumn("order_date", expr("date_sub(current_date(), CAST(rand() * 365 AS INT))")) \
    .withColumn("country", expr("CASE WHEN rand() < 0.5 THEN 'US' ELSE 'UK' END")) \
    .withColumn("total", round((col("quantity") * col("price")) * (1 - col("discount")), 2))


# COMMAND ----------

n_products = 1_000_000

product_df = spark.range(1, n_products + 1).withColumnRenamed("id", "product_id") \
    .withColumn("product_name", expr("concat('Product_', product_id)")) \
    .withColumn("category", expr("CASE WHEN product_id % 2 = 0 THEN 'Electronics' ELSE 'Clothing' END")) \
    .withColumn("price_base", round(rand() * 100 + 10, 2))


# COMMAND ----------

joined_df = sales_df.join(product_df, on="product_id", how="inner")
joined_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###one more example

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled','true')

# COMMAND ----------

from pyspark.sql.functions import expr, col, rand, round, lit
from pyspark.sql import functions as F

# Simulate 50 million rows (~10GB, rough estimate)
sales_df = spark.range(0, 50_000_000).withColumnRenamed("id", "order_id") \
    .withColumn("product_id", expr("CASE WHEN rand() < 0.9 THEN 1 ELSE CAST(rand() * 10000 AS INT) END")) \
    .withColumn("quantity", expr("CAST(rand() * 10 + 1 AS INT)")) \
    .withColumn("price", round(rand() * 100 + 1, 2)) \
    .withColumn("discount", round(rand() * 0.2, 2)) \
    .withColumn("order_date", expr("date_sub(current_date(), CAST(rand() * 365 AS INT))"))


# COMMAND ----------

product_df = spark.range(0, 10_000_000).withColumnRenamed("id", "product_id") \
    .withColumn("product_name", expr("concat('Product_', product_id)")) \
    .withColumn("category", expr("CASE WHEN rand() < 0.5 THEN 'A' ELSE 'B' END")) \
    .withColumn("price", round(rand() * 100 + 1, 2))


# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) 

# COMMAND ----------

joined_df = sales_df.join(product_df, on="product_id", how="inner")
joined_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###dynamically optimizing skewed join
# MAGIC

# COMMAND ----------

spark.conf.get("spark.sql.adaptive.skewJoin.enabled")

# COMMAND ----------

from pyspark.sql.functions import expr, rand, round

# Create skewed product_id (90% of rows = 1)
sales_df = spark.range(0, 100_000_000).withColumnRenamed("id", "order_id") \
    .withColumn("product_id", expr("CASE WHEN rand() < 0.9 THEN 1 ELSE CAST(rand() * 100000 AS INT) END")) \
    .withColumn("quantity", expr("CAST(rand() * 10 + 1 AS INT)")) \
    .withColumn("price", round(rand() * 100 + 1, 2)) \
    .withColumn("discount", round(rand() * 0.2, 2)) \
    .withColumn("order_date", expr("date_sub(current_date(), CAST(rand() * 365 AS INT))"))


# COMMAND ----------

product_df = spark.range(1, 1_000_000).withColumnRenamed("id", "product_id") \
    .withColumn("product_name", expr("concat('Product_', product_id)")) \
    .withColumn("category", expr("CASE WHEN rand() < 0.5 THEN 'A' ELSE 'B' END")) \
    .withColumn("price", round(rand() * 100 + 1, 2))


# COMMAND ----------

sales_df.join(product_df, on="product_id", how="inner").count()

# COMMAND ----------

# Enable AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Disable broadcast to force shuffle join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")

# Enable skew join optimization
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Lower the skew threshold to force detection
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", 1 * 1024 * 1024)  # 1MB
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", 2)


# COMMAND ----------

from pyspark.sql.functions import expr, rand, round, col, concat_ws

# Add a single rand() column to use
sales_df = spark.range(0, 80_000_000).withColumnRenamed("id", "order_id") \
    .withColumn("rand_val", rand()) \
    .withColumn("product_id", expr("CASE WHEN rand_val < 0.9 THEN 1 ELSE CAST(rand_val * 100000 + 2 AS INT) END")) \
    .drop("rand_val") \
    .withColumn("quantity", expr("CAST(rand() * 10 + 1 AS INT)")) \
    .withColumn("price", round(rand() * 100 + 1, 2)) \
    .withColumn("discount", round(rand() * 0.2, 2)) \
    .withColumn("order_date", expr("date_sub(current_date(), CAST(rand() * 365 AS INT))"))

salted_sales_df = sales_df.withColumn("salt", floor(rand() * 10))
salted_sales_df = salted_sales_df.withColumn("salted_product_id", concat_ws("_", col("product_id"), col("salt")))



# Smaller product_df — dimension table
product_df = spark.range(1, 100_000).withColumnRenamed("id", "product_id") \
    .withColumn("product_name", expr("concat('Product_', product_id)")) \
    .withColumn("category", expr("CASE WHEN rand() < 0.5 THEN 'A' ELSE 'B' END")) \
    .withColumn("brand", expr("concat('Brand_', CAST(rand() * 100 AS INT))"))

salt_range = spark.range(0, 10).withColumnRenamed("id", "salt")
salted_product_df = product_df.crossJoin(salt_range)
salted_product_df = salted_product_df.withColumn("salted_product_id", concat_ws("_", col("product_id"), col("salt")))

salted_sales_df.join(salted_product_df, on="salted_product_id", how="inner").count()

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", 5 * 1024 * 1024)  # 10MB
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", 3)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)  # Prevent broadcast to force shuffle join


# COMMAND ----------

# Lower the advisory partition size, so Spark knows the 9.7 MiB is “big enough” to consider for split
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", 4 * 1024 * 1024)  # 4MB


# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", 4 * 1024 * 1024)  # 4MB for testing
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", 4 * 1024 * 1024)  # 4MB
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", 5)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)  # force shuffle join
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")


from pyspark.sql.functions import expr, rand, round, col, floor
from pyspark.sql import SparkSession

# Create skewed sales_df: 100 million rows, 90% with product_id = 1
from pyspark.sql.functions import when, lit, repeat

# Create a large string (e.g., 10 KB)
large_str = repeat(lit("x"), 1024 * 1024)

sales_df = spark.range(0, 100_000_000).withColumnRenamed("id", "order_id") \
    .withColumn("rand_val", rand()) \
    .withColumn("product_id", expr("CASE WHEN rand_val < 0.9 THEN 1 ELSE CAST(rand_val * 100000 + 2 AS INT) END")) \
    .drop("rand_val") \
    .withColumn("quantity", expr("CAST(rand() * 10 + 1 AS INT)")) \
    .withColumn("price", round(rand() * 100 + 1, 2)) \
    .withColumn("discount", round(rand() * 0.2, 2)) \
    .withColumn("order_date", expr("date_sub(current_date(), CAST(rand() * 365 AS INT))")) \
    .withColumn("big_column", when(col("product_id") == 1, large_str).otherwise(lit("")))

sales_df = sales_df.repartition("product_id")  # Prevents early flattening of skew


# Create product_df (dimension table)
product_df = spark.range(1, 100_000).withColumnRenamed("id", "product_id") \
    .withColumn("product_name", expr("concat('Product_', product_id)")) \
    .withColumn("category", expr("CASE WHEN rand() < 0.5 THEN 'A' ELSE 'B' END"))

# Disable broadcast join so Spark performs shuffle join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# Join — this will force shuffle and reveal skew in Spark UI
result_df = sales_df.join(product_df, on="product_id", how="inner")
result_df.count()


# COMMAND ----------

spark.conf.get("spark.sql.adaptive.advisoryPartitionSizeInBytes")

# COMMAND ----------

print('spark.sql.adaptive.enabled',spark.conf.get("spark.sql.adaptive.enabled"))
print('spark.sql.adaptive.skewJoin.enabled',spark.conf.get("spark.sql.adaptive.skewJoin.enabled"))
print("spark.sql.adaptive.coalescePartitions.enabled",spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled"))

# COMMAND ----------

print("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"), '256MB')
print("spark.sql.adaptive.advisoryPartitionSizeInBytes",spark.conf.get("spark.sql.adaptive.advisoryPartitionSizeInBytes"), '64MB')
print("spark.sql.adaptive.skewJoin.skewedPartitionFactor",spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor"))
print("spark.sql.autoBroadcastJoinThreshold",spark.conf.get("spark.sql.autoBroadcastJoinThreshold"), '10MB')
print("spark.sql.adaptive.autoBroadcastJoinThreshold",spark.conf.get("spark.sql.adaptive.autoBroadcastJoinThreshold"), '10MB')
