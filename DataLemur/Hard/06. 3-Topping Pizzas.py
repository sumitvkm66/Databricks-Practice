# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName('dl').getOrCreate()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DecimalType

schema = StructType([
    StructField('topping_name', StringType()),
    StructField('ingredient_cost', FloatType())
])

data = [
    ("Pepperoni", 0.50),
    ("Sausage", 0.70),
    ("Chicken", 0.55),
    ("Extra Cheese", 0.40)
]

# COMMAND ----------

pizza_toppings = spark.createDataFrame(data, schema)
pizza_toppings.show()

# COMMAND ----------

p1 = pizza_toppings.alias('p1')
p2 = pizza_toppings.alias('p2')
p3 = pizza_toppings.alias('p3')

p1.join(p2, col('p1.topping_name')<col('p2.topping_name'),'inner')\
    .join(p3, col('p2.topping_name')<col('p3.topping_name'),'inner')\
    .select(
        concat('p1.topping_name',lit(','),'p2.topping_name',lit(','),'p3.topping_name').alias('pizza'),
        round(col('p1.ingredient_cost')+col('p2.ingredient_cost')+col('p3.ingredient_cost'),2).alias('total_cost')
        )\
    .orderBy(col('total_cost').desc(), 'pizza')\
    .show(truncate=False)
