# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, rand, randn, floor
import random
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("SalesData").getOrCreate()

# Total rows
total_rows = 1000
sugar_rows = int(0.8 * total_rows)
other_rows = total_rows - sugar_rows

# Products and price mapping
product_price_map = {
    'Sugar': 40,
    'Salt': 20,
    'Rice': 60,
    'Oil': 100,
    'Wheat': 50
}
other_products = list(product_price_map.keys())
other_products.remove("Sugar")

# Create product list
products = ['Sugar'] * sugar_rows + random.choices(other_products, k=other_rows)
random.shuffle(products)

# Create base data as list of dicts
data = []
start_date = datetime.today() - timedelta(days=60)

for i in range(total_rows):
    product = products[i]
    price = product_price_map[product]
    quantity = random.randint(1, 10)
    sales = price * quantity
    customer_id = random.randint(1000, 1100)
    region = random.choice(['North', 'South', 'East', 'West'])
    sale_date = start_date + timedelta(days=random.randint(0, 59))

    data.append((product, price, quantity, sales, customer_id, region, sale_date))

# Create Spark DataFrame
columns = ["product", "price", "quantity", "sales", "customer_id", "region", "date"]
sales_df = spark.createDataFrame(data, schema=columns)

# Show sample
sales_df.show()

# COMMAND ----------

sales_df.groupBy("product").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Dynamically switching join

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled','true')

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Create SparkSession with AQE enabled
spark = SparkSession.builder \
    .appName("AQE Broadcast Join Example") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.join.enabled", "true") \
    .config("spark.sql.adaptive.logLevel", "INFO") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")\  
    .getOrCreate()

# Sample large sales DataFrame
sales_data = [
    (1, "Sugar", 100),
    (2, "Rice", 200),
    (3, "Oil", 300),
    (4, "Sugar", 400),
    (5, "Salt", 500)
]
sales_df = spark.createDataFrame(sales_data, ["sale_id", "product", "amount"])

# Sample product DataFrame that might be small or large
product_data = [
    ("Sugar", "Sweetener"),
    ("Rice", "Grain"),
    ("Oil", "Cooking Oil"),
    ("Salt", "Seasoning")
]
product_df = spark.createDataFrame(product_data, ["product", "category"])


joined_df = sales_df.join(product_df, on="product", how="inner")

joined_df.show()


# COMMAND ----------


