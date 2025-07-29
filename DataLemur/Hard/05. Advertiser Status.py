# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('dl').getOrCreate()
from pyspark.sql.types import *

# COMMAND ----------

# Define schema
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("status", StringType(), True)
])

# Define data
data = [
    ("bing", "NEW"),
    ("yahoo", "NEW"),
    ("alibaba", "EXISTING"),
    ("baidu", "EXISTING"),
    ("target", "CHURN"),
    ("tesla", "CHURN"),
    ("morgan", "RESURRECT"),
    ("chase", "RESURRECT")
]

# Define schema
schema_paid = StructType([
    StructField("user_id", StringType(), True),
    StructField("paid", DoubleType(), True)
])

# Define data
data_paid = [
    ("yahoo", 45.00),
    ("alibaba", 100.00),
    ("target", 13.00),
    ("morgan", 600.00),
    ("fitdata", 25.00)
]

# COMMAND ----------

advertiser = spark.createDataFrame(data, schema)
daily_pay = spark.createDataFrame(data_paid, schema_paid)
advertiser.show()
daily_pay.show()

# COMMAND ----------

advertiser.alias('a').join(daily_pay.alias('b'), 'user_id','full')\
    .select('user_id',
            when(col('paid').isNull(),'CHURN')
            .when(
                col('status').isin('EXISTING','NEW','RESURRECT') & col('paid').isNotNull(), 
                'EXISTING'
                )
            .when(
                (col('status')=='CHURN') & (col('paid').isNotNull()),
                'RESURRECT'
            )
            .when(
                col('a.user_id').isNull() & col('paid').isNotNull(),
                'NEW'
            )
            .alias('new_status')
    )\
    .orderBy('user_id')\
    .show()

# COMMAND ----------

advertiser.alias('a').join(daily_pay.alias('b'), 'user_id','full')\
    .selectExpr('coalesce(a.user_id,b.user_id) as user_id',
                "case when b.paid is null then 'CHURN'\
    when a.status IN ('EXISTING','NEW','RESURRECT') AND b.paid is not null then 'EXISTING'\
    when a.status = 'CHURN' and b.paid is not null then 'RESURRECT'\
    when a.user_id is null and b.paid is not null then 'NEW'\
    END AS new_status"
                )\
    .orderBy('user_id')\
    .show()
