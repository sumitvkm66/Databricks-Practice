# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('leetcode').getOrCreate()

# COMMAND ----------

schema_purchases = ["user_id", "product_id", "quantity"]
data_purchases = [
    (1, 101, 2), (1, 102, 1), (1, 103, 3),
    (2, 101, 1), (2, 102, 5), (2, 104, 1),
    (3, 101, 2), (3, 103, 1), (3, 105, 4),
    (4, 101, 1), (4, 102, 1), (4, 103, 2), (4, 104, 3),
    (5, 102, 2), (5, 104, 1)
]
schema_info = ["product_id", "category", "price"]
data_info = [
    (101, "Electronics", 100),
    (102, "Books", 20),
    (103, "Clothing", 35),
    (104, "Kitchen", 50),
    (105, "Sports", 75)
]


# COMMAND ----------

purchase = spark.createDataFrame(data_purchases, schema_purchases)
products = spark.createDataFrame(data_info,schema_info)
purchase.show()
products.show()

# COMMAND ----------

product_pairs = products.alias('a').join(products.alias('b'), col('a.product_id')<col('b.product_id'),how='inner')\
    .selectExpr('a.product_id as product1_id','b.product_id as product2_id','a.category as product1_category','b.category as product2_category')

product_pairs.alias('a').join(purchase.alias('b'), col('a.product1_id')==col('b.product_id'), how='inner')\
    .join(purchase.alias('c'), col('a.product2_id')==col('c.product_id'), how='inner')\
    .filter(col('b.user_id')==col('c.user_id'))\
    .groupBy('product1_id','product2_id','product1_category','product2_category').agg(
        count("*").alias('customer_count')
    )\
    .filter(col('customer_count')>2)\
    .orderBy('customer_count','product1_id','product2_id')\
    .show()
