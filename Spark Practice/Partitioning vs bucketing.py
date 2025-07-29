# Databricks notebook source
# MAGIC %fs 
# MAGIC ls /FileStore

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True),
    StructField("address", StringType(), True),
    StructField("gender", StringType(), True)
])

df = spark.read.format('csv')\
    .option('header','true')\
    .schema(schema)\
    .load('dbfs:/FileStore/employee/')



df.filter('id is not null').repartition(1).write.format('csv')\
    .option('header','true')\
    .mode('overwrite')\
    .save('/FileStore/employee')

# COMMAND ----------

df = spark.read.format('csv')\
    .option('header','true')\
    .option('inferSchema','true')\
    .load('dbfs:/FileStore/employee/')

df.count()

# COMMAND ----------

df.write.format('csv')\
    .option('header','true')\
    .mode('overwrite')\
    .partitionBy('address')\
    .save('/FileStore/tables/employee_by_address')

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/employee_by_address/address=INDIA/

# COMMAND ----------

spark.read.format('csv')\
    .option('header','true')\
    .option("inferSchema", "true")\
    .load('/FileStore/tables/employee_by_address')\
    .filter('address=="INDIA"')\
    .explain()

# COMMAND ----------

df.write.format('csv')\
    .option('header','true')\
    .mode('overwrite')\
    .partitionBy('address','gender')\
    .save('/FileStore/tables/employee_by_address_gender')

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/employee_by_address_gender

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/employee_by_address_gender/address=INDIA/

# COMMAND ----------

df.write.format('csv')\
    .option('header','true')\
    .mode('overwrite')\
    .option('path','/FileStore/tables/bucket_by_id')\
    .bucketBy(3,'id')\
    .sortBy('id')\
    .saveAsTable('bucket_by_id')

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/bucket_by_id

# COMMAND ----------

spark.read.format('csv')\
    .option('header','true')\
    .load('dbfs:/FileStore/tables/bucket_by_id/part-00000-tid-6539054070851890430-c514d1bb-6a28-40b4-baf0-7c4a8c467370-244-1_00000.c000.csv')\
    .show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bucket_by_id

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled','false')

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
import os

# Setup SparkSession with Hive support
spark = SparkSession.builder \
    .appName("BucketingExample") \
    .config("spark.sql.shuffle.partitions", "8") \
    .enableHiveSupport() \
    .getOrCreate()

# Create directories
output_dir = "/tmp/spark_bucketing_example"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Simulated orders table (fact)
orders = spark.range(0, 1_000_000).withColumn("customer_id", (col("id") % 1000)) \
                                   .withColumn("amount", expr("round(rand() * 100, 2)"))

# Simulated customers table (dimension)
customers = spark.range(0, 1000).withColumnRenamed("id", "customer_id") \
                                .withColumn("name", expr("concat('Customer_', customer_id)"))

# Save customers as bucketed table
customers.write.format('csv') \
    .option('header','true')\
    .bucketBy(8, "customer_id") \
    .sortBy("customer_id") \
    .mode("overwrite") \
    .option('path','/FileStore/tables/bucketed_customers')\
    .saveAsTable("bucketed_customers")

# Save orders as bucketed table
orders.write.format('csv') \
    .option('header','true')\
    .bucketBy(8, "customer_id") \
    .sortBy("customer_id") \
    .mode("overwrite") \
    .option('path','/FileStore/tables/bucketed_orders')\
    .saveAsTable("bucketed_orders")

# 🔁 Read back the tables
bucketed_orders = spark.table("bucketed_orders")
bucketed_customers = spark.table("bucketed_customers")

# ✅ Optimized join: Spark uses bucket metadata
result = bucketed_orders.join(bucketed_customers, on="customer_id")

print("Partitions in join result:", result.rdd.getNumPartitions())

result.select("customer_id", "amount", "name").show(10)

