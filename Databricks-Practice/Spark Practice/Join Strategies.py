# Databricks notebook source
from pyspark.sql import Row

customers_data = [
    Row(customer_id=1, name="Alice", country="USA"),
    Row(customer_id=2, name="Bob", country="Canada"),
    Row(customer_id=3, name="Charlie", country="USA"),
    Row(customer_id=4, name="Diana", country="UK"),
    Row(customer_id=5, name="Ethan", country="Canada")
]

customers_df = spark.createDataFrame(customers_data)
customers_df.show()


# COMMAND ----------

sales_data = [
    Row(sale_id=101, customer_id=1, amount=200.0),
    Row(sale_id=102, customer_id=2, amount=150.0),
    Row(sale_id=103, customer_id=1, amount=300.0),
    Row(sale_id=104, customer_id=3, amount=120.0),
    Row(sale_id=105, customer_id=5, amount=500.0),
    Row(sale_id=106, customer_id=6, amount=250.0)  # customer not in customers_df
]

sales_df = spark.createDataFrame(sales_data)
sales_df.show()


# COMMAND ----------

spark.conf.get('spark.sql.shuffle.partitions')

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled",'true')  # Should be true
#When we set it to false, by default 200 partitions will be created by spark for each wide dependency transformation

# COMMAND ----------

sort_merge_df = customers_df.join(sales_df, customers_df.customer_id==sales_df.customer_id, 'inner')
sort_merge_df.show()
sort_merge_df.explain()

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", '10485760b')

# COMMAND ----------

from pyspark.sql.functions import broadcast
sort_merge_df = customers_df.join(broadcast(sales_df), customers_df.customer_id==sales_df.customer_id, 'inner')
sort_merge_df.show()
sort_merge_df.explain()

# COMMAND ----------

spark.conf.get('spark.sql.autoBroadcastJoinThreshold')
