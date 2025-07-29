# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, expr, col, floor
from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType, StructField, DateType
import random
from datetime import datetime, timedelta

# Create Spark session
spark = SparkSession.builder.appName("SalesDataGenerator").getOrCreate()

# Helper function to generate random dates
def random_date(start_date, end_date):
    delta = end_date - start_date
    return start_date + timedelta(days=random.randint(0, delta.days))

# Create sample data
num_records = 1000
start_date = datetime(2022, 1, 1)
end_date = datetime(2024, 12, 31)

regions = ["North", "South", "East", "West", "Central"]
products = list(range(1001, 1011))  # 10 product IDs

data = []

for i in range(num_records):
    customer_id = random.randint(1, 300)
    product_id = random.choice(products)
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(10, 200), 2)
    total_cost = round(quantity * unit_price, 2)
    sales_date = random_date(start_date, end_date)
    region = random.choice(regions)

    data.append((customer_id, product_id, quantity, unit_price, total_cost, sales_date, region))

# Define schema
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("total_cost", DoubleType(), True),
    StructField("sales_date", DateType(), True),
    StructField("region", StringType(), True),
])

# Create DataFrame
sales_df = spark.createDataFrame(data, schema)

# Show sample
sales_df.show(10, truncate=False)


# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/sales/sales_date=2023-03-05/

# COMMAND ----------

sales_df.write.format('csv')\
    .option('header','true')\
    .option('path','/FileStore/tables/sales')\
    .partitionBy('sales_date')\
    .save()

# COMMAND ----------

sales = spark.read.format('csv')\
    .option('header','true')\
    .load('/FileStore/tables/sales')

sales.filter('sales_date=="2023-03-05"').show()

# COMMAND ----------

from pyspark.sql.functions import col, expr, to_date, date_format, dayofweek, dayofmonth, dayofyear, weekofyear, month, year
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

# Initialize Spark session (if not already initialized)
spark = SparkSession.builder.appName("DateDimension").getOrCreate()

# Define start and end date
start_date = datetime(2022, 1, 1)
end_date = datetime(2024, 12, 31)

# Generate a list of dates
date_list = [(start_date + timedelta(days=x),) for x in range((end_date - start_date).days + 1)]

# Create DataFrame from the date list
date_df = spark.createDataFrame(date_list, ["date"])

# Add date dimension columns
date_dim_df = (
    date_df
    .withColumn("day", dayofmonth("date"))
    .withColumn("day_of_week", dayofweek("date"))  # Sunday=1, Saturday=7
    .withColumn("day_name", date_format("date", "EEEE"))
    .withColumn("week_of_year", weekofyear("date"))
    .withColumn("month", month("date"))
    .withColumn("month_name", date_format("date", "MMMM"))
    .withColumn("quarter", expr("quarter(date)"))
    .withColumn("year", year("date"))
    .withColumn("day_of_year", dayofyear("date"))
    .withColumn("is_weekend", expr("dayofweek(date) IN (1, 7)"))
)

# Show sample
date_dim_df.show(10, truncate=False)


# COMMAND ----------

date_dim_df.write.format('csv')\
    .option('header','true')\
    .save('/FileStore/tables/dates')

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/dates

# COMMAND ----------

spark.conf.set('spark.sql.optimizer.dynamicPartitionPruning.enabled','true')
spark.conf.set('spark.sql.autoBroadcastJoinThreshold','10m')

# COMMAND ----------

spark.conf.get('spark.sql.autoBroadcastJoinThreshold')

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

sales = spark.read.format('csv')\
    .option('header','true')\
    .load('/FileStore/tables/sales')

dates = spark.read.format('csv')\
    .option('header','true')\
    .load('/FileStore/tables/dates')

sales.join(dates, sales.sales_date==col("date").cast('date'), 'inner')\
    .filter('week_of_year==9').show()
