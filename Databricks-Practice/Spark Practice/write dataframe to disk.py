# Databricks notebook source
employee = spark.read.format('csv')\
    .option('header','true')\
    .load('/FileStore/employee/')

# COMMAND ----------

employee.rdd.getNumPartitions()

# COMMAND ----------

employee.write.format('csv')\
    .mode('overwrite')\
    .option('header', 'true')\
    .option('compression','gzip')\
    .save('/FileStore/employee/')


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/employee/

# COMMAND ----------

# MAGIC %md
# MAGIC ####Generate 10GB of data

# COMMAND ----------

from pyspark.sql.functions import expr, col, rand, round, monotonically_increasing_id
from pyspark.sql.types import *
import math

# Estimate number of rows — depends on column types
# Roughly ~200 bytes per row → 10GB / 200B ≈ 50 million rows
n_rows = 50_000_000

# Generate base DataFrame with sequential IDs
df = spark.range(0, n_rows).withColumnRenamed("id", "order_id")

# Add random columns
df = df.withColumn("customer_id", expr("CAST(rand() * 100000 AS INT)")) \
       .withColumn("product_id", expr("CAST(rand() * 1000 AS INT)")) \
       .withColumn("quantity", expr("CAST(rand() * 10 + 1 AS INT)")) \
       .withColumn("price", round(rand() * 100 + 1, 2)) \
       .withColumn("discount", round(rand() * 0.2, 2)) \
       .withColumn("order_date", expr("date_sub(current_date(), CAST(rand() * 365 AS INT))")) \
       .withColumn("country", expr("CASE WHEN rand() < 0.5 THEN 'US' ELSE 'UK' END"))

# Calculate total
df = df.withColumn("total", round((col("quantity") * col("price")) * (1 - col("discount")), 2))


# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df.write.option('header','true')\
    .mode('overwrite')\
    .parquet('/FileStore/tables/sales_10gb_compressed')


# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/sales_10gb_compressed

# COMMAND ----------

df.write.option('header','true')\
    .mode('overwrite')\
    .csv('/FileStore/tables/sales_10gb_uncompressed_csv')

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/sales_10gb_uncompressed_csv

# COMMAND ----------

df.write.option('header','true')\
    .mode('overwrite')\
    .option('compression','gzip')\
    .csv('/FileStore/tables/sales_10gb_compressed_csv')

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/sales_10gb_compressed_csv

# COMMAND ----------

from pyspark.sql.functions import expr, col, rand, round, monotonically_increasing_id
from pyspark.sql.types import *
import math

# Estimate number of rows — depends on column types
# Roughly ~200 bytes per row → 10GB / 200B ≈ 50 million rows
n_rows = 50_000_000

# Generate base DataFrame with sequential IDs
df = spark.range(0, n_rows).withColumnRenamed("id", "order_id")

# Add random columns
df = df.withColumn("customer_id", expr("CAST(rand() * 100000 AS INT)")) \
       .withColumn("product_id", expr("CAST(rand() * 1000 AS INT)")) \
       .withColumn("quantity", expr("CAST(rand() * 10 + 1 AS INT)")) \
       .withColumn("price", round(rand() * 100 + 1, 2)) \
       .withColumn("discount", round(rand() * 0.2, 2)) \
       .withColumn("order_date", expr("date_sub(current_date(), CAST(rand() * 365 AS INT))")) \
       .withColumn("country", expr("CASE WHEN rand() < 0.5 THEN 'US' ELSE 'UK' END"))

# Calculate total
sales_df = df.withColumn("total", round((col("quantity") * col("price")) * (1 - col("discount")), 2))


# COMMAND ----------

sales_df.rdd.getNumPartitions()

# COMMAND ----------

# Estimate: ~33M rows for 1GB
rows_1gb = int((1 * 1024**3) / 32)

product_df = spark.range(0, rows_1gb) \
    .withColumnRenamed("id", "product_id") \
    .withColumn("product_name", expr("concat('Product_', product_id)"))


# COMMAND ----------

product_df.rdd.getNumPartitions()

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled','true')

# COMMAND ----------

joined_df = sales_df.join(product_df, on="product_id", how="inner")
joined_df.count()  # or joined_df.write.mode("overwrite").parquet("/tmp/joined_output")
