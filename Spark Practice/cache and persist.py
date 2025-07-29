# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

spark = SparkSession.builder \
    .appName("CachePersistExample") \
    .getOrCreate()

# Sample data
data = [(i, f"product_{i % 5}") for i in range(1000000)]
df = spark.createDataFrame(data, ["id", "product"])

# Expensive transformation
transformed_df = df.withColumn("id_squared", df["id"] * df["id"])
# transformed_df.show()

# COMMAND ----------

# Example 1: CACHE
# Store in memory + disk
transformed_df.cache()

# COMMAND ----------

transformed_df.count()  # This triggers the caching

# COMMAND ----------

# You can unpersist when done
transformed_df.unpersist()

# COMMAND ----------

# Example 2: PERSIST
# Store only on disk (simulate limited memory)
persisted_df = transformed_df.persist(StorageLevel.DISK_ONLY)

# COMMAND ----------

persisted_df.count()

# COMMAND ----------

persisted_df.unpersist()

# COMMAND ----------

# You can unpersist when done
persisted_df.unpersist()

# COMMAND ----------

spark.catalog.clearCache()
