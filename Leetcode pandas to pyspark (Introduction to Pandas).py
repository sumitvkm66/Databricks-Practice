# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###Drop duplicates and sorting

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("DatabricksDataFrame").getOrCreate()

# Define the data as a list of tuples
data = [
    (1, "Ella", "emily@example.com"),
    (2, "David", "michael@example.com"),
    (3, "Zachary", "sarah@example.com"),
    (4, "Alice", "john@example.com"),
    (5, "Finn", "john@example.com"),
    (6, "Violet", "alice@example.com"),
]

# Define the column names
columns = ["customer_id", "name", "email"]

# Create the DataFrame
customers = spark.createDataFrame(data, schema=columns)

# Display the DataFrame
customers.show()


# COMMAND ----------

customers.dropDuplicates(['email']).orderBy('customer_id', col('name').desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###dropna fillna

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("StudentsDataFrame").getOrCreate()

# Define the data as a list of tuples
data = [
    (32, "Piper", 5),
    (217, None, 19),  # None represents null
    (779, "Georgia", 20),
    (849, "Willow", 14),
]

# Define the column names
columns = ["student_id", "name", "age"]

# Create the DataFrame
students = spark.createDataFrame(data, schema=columns)

# Display the DataFrame
students.show()


# COMMAND ----------

students.dropna(subset=['name']).show()

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("ProductsDataFrame").getOrCreate()

# Define the data as a list of tuples
data = [
    ("Wristwatch", None, 135),
    ("WirelessEarbuds", None, 821),
    ("GolfClubs", 779, 9319),
    ("Printer", 849, 3051),
]

# Define the column names
columns = ["name", "quantity", "price"]

# Create the DataFrame
products = spark.createDataFrame(data, schema=columns)

# Display the DataFrame
products.show()


# COMMAND ----------

products.fillna(0,subset='quantity').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Add column

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("EmployeesDataFrame").getOrCreate()

# Define the data as a list of tuples
data = [
    ("Jack", 19666),
    ("Piper", 74754),
    ("Mia", 62509),
    ("Ulysses", 54866),
]

# Define the column names
columns = ["name", "salary"]

# Create the DataFrame
employees = spark.createDataFrame(data, schema=columns)

# Display the DataFrame
employees.show()


# COMMAND ----------

employees.withColumn('salary',col('salary')*2).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###rename columns

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("StudentsDataFrame").getOrCreate()

# Define the data as a list of tuples
data = [
    (1, "Mason", "King", 6),
    (2, "Ava", "Wright", 7),
    (3, "Taylor", "Hall", 16),
    (4, "Georgia", "Thompson", 18),
    (5, "Thomas", "Moore", 10),
]

# Define the column names
columns = ["id", "first", "last", "age"]

# Create the DataFrame
students = spark.createDataFrame(data, schema=columns)

# Display the DataFrame
students.show()


# COMMAND ----------

columns = {
    'id':'student_id',
    'first':'first_name',
    'last':'last_name',
    'age':'age_in_years'
}
for old, new in columns.items():
    students = `students.withColumnRenamed(old,new)

students.show()    

# COMMAND ----------

# MAGIC %md
# MAGIC ###change data types

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("StudentsDataFrame").getOrCreate()

# Define the data as a list of tuples
data = [
    (1, "Ava", 6, 73),
    (2, "Kate", 15, 87)
]

# Define the column names
columns = ["student_id", "name", "age", "grade"]

# Create the DataFrame
students = spark.createDataFrame(data, schema=columns)

# Display the DataFrame
students.show()


# COMMAND ----------

students = students.withColumn('grade',col('grade').cast(IntegerType()))

students.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###concat/union dataframes

# COMMAND ----------

# Define data for df1
data1 = [
    (1, "Mason", 8),
    (2, "Ava", 6),
    (3, "Taylor", 15),
    (4, "Georgia", 17),
    (5, "Leo", 7),
]

# Define data for df2
data2 = [
    (5, "Leo", 7),
    (6, "Alex", 7)
]

# Define column names
columns = ["student_id", "name", "age1"]

# Create DataFrames
df1 = spark.createDataFrame(data1, schema=columns)
df2 = spark.createDataFrame(data2, schema=columns)

# Display df1
df1.show()

# Display df2
df2.show()


# COMMAND ----------

df1.union(df2).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###PIVOT

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("WeatherDataFrame").getOrCreate()

# Define the data as a list of tuples
data = [
    ("Jacksonville", "January", 13),
    ("Jacksonville", "February", 23),
    ("Jacksonville", "March", 38),
    ("Jacksonville", "April", 5),
    ("Jacksonville", "May", 34),
    ("ElPaso", "January", 20),
    ("ElPaso", "February", 6),
    ("ElPaso", "March", 26),
    ("ElPaso", "April", 2),
    ("ElPaso", "May", 43),
]

# Define the column names
columns = ["city", "month", "temperature"]

# Create the DataFrame
weather = spark.createDataFrame(data, schema=columns)

# Display the DataFrame
weather.show()


# COMMAND ----------

weather.groupBy('month').pivot('city').max('temperature').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###UNPIVOT

# COMMAND ----------

weather1 = weather.groupBy('month').pivot('city').max('temperature')
weather1.show()

# COMMAND ----------

weather1.melt(ids='month',values=['ElPaso','Jacksonville'],variableColumnName='city',valueColumnName='temperature')\
    .select(['city','month','temperature']).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###method chaining/filter

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("AnimalsDataFrame").getOrCreate()

# Define the data as a list of tuples
data = [
    ("Tatiana", "Snake", 98, 464),
    ("Khaled", "Giraffe", 50, 41),
    ("Alex", "Leopard", 6, 328),
    ("Jonathan", "Monkey", 45, 463),
    ("Stefan", "Bear", 100, 50),
    ("Tommy", "Panda", 26, 349)
]

# Define the column names
columns = ["name", "species", "age", "weight"]

# Create the DataFrame
animals = spark.createDataFrame(data, schema=columns)

# Display the DataFrame
animals.show()


# COMMAND ----------

animals.filter(col('weight')>100).show()
