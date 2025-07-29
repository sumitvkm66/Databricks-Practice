# Databricks notebook source
# MAGIC %fs ls dbfs:/user/hive/warehouse/transactions/_delta_log/

# COMMAND ----------

transactions = spark.read.format('csv')\
    .option('header','true')\
    .option('inferSchema','true')\
    .load('dbfs:/FileStore/sample_transactions-1.csv')

transactions.show()

# COMMAND ----------

transactions.write.format('delta').mode('overwrite').saveAsTable('transactions')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from transactions limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM transactions
# MAGIC WHERE txn_time BETWEEN '2025-06-01' AND '2025-07-01';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO transactions VALUES
# MAGIC ('TXN1001', 'CUST1234', '2025-07-01 10:30:00', 150.75, 'SUCCESS'),
# MAGIC ('TXN1002', 'CUST1235', '2025-07-02 14:45:00', 89.99, 'FAILED'),
# MAGIC ('TXN1003', 'CUST1236', '2025-07-03 09:10:00', 240.00, 'SUCCESS'),
# MAGIC ('TXN1004', 'CUST1237', '2025-07-01 18:00:00', 325.50, 'PENDING'),
# MAGIC ('TXN1005', 'CUST1238', '2025-06-30 11:25:00', 670.30, 'SUCCESS'),
# MAGIC ('TXN1006', 'CUST1239', '2025-07-04 15:15:00', 410.10, 'FAILED'),
# MAGIC ('TXN1007', 'CUST1240', '2025-07-05 08:05:00', 199.99, 'SUCCESS'),
# MAGIC ('TXN1008', 'CUST1241', '2025-07-05 21:00:00', 520.20, 'PENDING'),
# MAGIC ('TXN1009', 'CUST1242', '2025-06-29 13:30:00', 310.45, 'FAILED'),
# MAGIC ('TXN1010', 'CUST1243', '2025-07-02 17:40:00', 750.00, 'SUCCESS');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM transactions
# MAGIC WHERE txn_time BETWEEN '2025-06-01' AND '2025-07-01';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize transactions zorder by (txn_time)

# COMMAND ----------

# MAGIC %md
# MAGIC Zorder demo

# COMMAND ----------

from pyspark.sql.functions import rand, expr
from pyspark.sql import functions as F

# Create a list of customer IDs (simulate skewed data)
customers = [f"CUST{1000 + i}" for i in range(10)]
customers_str = ",".join([f"'{c}'" for c in customers])  # Properly format for Spark SQL

# Generate demo DataFrame
df = spark.range(10000).withColumn("txn_id", F.expr("uuid()")) \
    .withColumn("customer_id", F.expr(f"element_at(array({customers_str}), cast(rand()*10 as int) + 1)")) \
    .withColumn("txn_time", F.expr("timestampadd(DAY, cast(rand()*540 as int), to_timestamp('2024-01-01'))")) \
    .withColumn("amount", (rand() * 2000).cast("double")) \
    .withColumn("status", expr("element_at(array('SUCCESS','FAILED','PENDING'), cast(rand()*3 as int) + 1)"))

# Save as Delta table
df.write.format("delta").mode("overwrite").saveAsTable("transactions_zorder_demo")


# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/transactions_zorder_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM transactions_zorder_demo
# MAGIC WHERE customer_id = 'CUST1002';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE transactions_zorder_demo
# MAGIC ZORDER BY (customer_id);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM transactions_zorder_demo
# MAGIC WHERE customer_id = 'CUST1002';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC time travel

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from transactions version as of 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from transactions timestamp as of "2025-07-05T09:54:55.000+00:00"

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from transactions where customer_id = 'CUST2298'

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table transactions to version as of 3

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO transactions VALUES
# MAGIC ('TXN1001', 'CUST1234', '2025-07-01 10:30:00', 150.75, 'SUCCESS'),
# MAGIC ('TXN1002', 'CUST1235', '2025-07-02 14:45:00', 89.99, 'FAILED'),
# MAGIC ('TXN1003', 'CUST1236', '2025-07-03 09:10:00', 240.00, 'SUCCESS'),
# MAGIC ('TXN1004', 'CUST1237', '2025-07-01 18:00:00', 325.50, 'PENDING'),
# MAGIC ('TXN1005', 'CUST1238', '2025-06-30 11:25:00', 670.30, 'SUCCESS'),
# MAGIC ('TXN1006', 'CUST1239', '2025-07-04 15:15:00', 410.10, 'FAILED'),
# MAGIC ('TXN1007', 'CUST1240', '2025-07-05 08:05:00', 199.99, 'SUCCESS'),
# MAGIC ('TXN1008', 'CUST1241', '2025-07-05 21:00:00', 520.20, 'PENDING'),
# MAGIC ('TXN1009', 'CUST1242', '2025-06-29 13:30:00', 310.45, 'FAILED'),
# MAGIC ('TXN1010', 'CUST1243', '2025-07-02 17:40:00', 750.00, 'SUCCESS');

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table transactions set tblproperties (delta.enableChangeDataFeed=true)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes('transactions',6) 

# COMMAND ----------

spark.read.format('delta')\
    .option('readChangeFeed','true')\
    .option('startingVersion',6)\
    .table('transactions')\
    .show()
