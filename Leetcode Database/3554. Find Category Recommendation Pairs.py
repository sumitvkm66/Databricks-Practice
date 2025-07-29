# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('leetcode').getOrCreate()

# COMMAND ----------

schema_purchases = ["user_id", "product_id", "quantity"]
data_purchases = [
    (1, 101, 2), (1, 102, 1), (1, 201, 3), (1, 301, 1),
    (2, 101, 1), (2, 102, 2), (2, 103, 1), (2, 201, 5),
    (3, 101, 2), (3, 103, 1), (3, 301, 4), (3, 401, 2),
    (4, 101, 1), (4, 201, 3), (4, 301, 1), (4, 401, 2),
    (5, 102, 2), (5, 103, 1), (5, 201, 2), (5, 202, 3)
]

schema_info = ["product_id", "category", "price"]
data_info = [
    (101, "Electronics", 100),
    (102, "Books", 20),
    (103, "Books", 35),
    (201, "Clothing", 45),
    (202, "Clothing", 60),
    (301, "Sports", 75),
    (401, "Kitchen", 50)
]


# COMMAND ----------

purchase = spark.createDataFrame(data_purchases,schema_purchases)
product = spark.createDataFrame(data_info,schema_info)
purchase.show()
product.show()

# COMMAND ----------

pairs = product.alias('a').join(product.alias('b'), col('a.category')<col('b.category'), 'inner')\
    .selectExpr('a.category as category1','b.category as category2').distinct()
purchases = purchase.alias('a').join(product.alias('p'), col('p.product_id')==col('a.product_id'), 'inner')\
    .selectExpr('a.user_id','p.category ')
pairs.alias('a').join(purchases.alias('p1'), col('a.category1')==col('p1.category'), 'inner')\
    .join(purchases.alias('p2'), col('a.category2')==col('p2.category'), 'inner')\
    .filter("p1.user_id==p2.user_id")\
    .groupBy('category1','category2').agg(countDistinct('p1.user_id').alias('CustomerCount'))\
    .filter('CustomerCount>=3')\
    .orderBy(col('CustomerCount').desc(), 'category1', 'category2')\
    .show()

# COMMAND ----------

products = product.alias('a').join(product.alias('b'), col('a.category')<col('b.category'), how='inner')\
.selectExpr('a.product_id as product1','a.category as category1','b.product_id as product2','b.category as category2')

products.alias('a').join(purchase.alias('b'), col('a.product1')==col('b.product_id'), how='inner')\
    .join(purchase.alias('c'), col('a.product2')==col('c.product_id'), how='inner')\
    .filter(col('b.user_id')==col('c.user_id'))\
    .groupBy('category1','category2').agg(countDistinct(col('b.user_id')).alias('customer_count'))\
    .filter(col('customer_count')>=3)\
    .orderBy(col('customer_count').desc(), 'category1','category2')\
    .show()
