# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ###595. Big Countries

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("WorldDataFrame").getOrCreate()

# Define the data as a list of tuples
data = [
    ("Afghanistan", "Asia", 652230, 25500100, 20343000000),
    ("Albania", "Europe", 28748, 2831741, 12960000000),
    ("Algeria", "Africa", 2381741, 37100000, 188681000000),
    ("Andorra", "Europe", 468, 78115, 3712000000),
    ("Angola", "Africa", 1246700, 20609294, 100990000000)
]

# Define the column names
columns = ["name", "continent", "area", "population", "gdp"]

# Create the DataFrame
world = spark.createDataFrame(data, schema=columns)

# Display the DataFrame
world.show()


# COMMAND ----------

world.filter((col('area')>=3000000) | (col('population')>=25000000)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###1757. Recyclable and Low Fat Products

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("ProductsDataFrame").getOrCreate()

# Define the data as a list of tuples
data = [
    (0, "Y", "N"),
    (1, "Y", "Y"),
    (2, "N", "Y"),
    (3, "Y", "Y"),
    (4, "N", "N")
]

# Define the column names
columns = ["product_id", "low_fats", "recyclable"]

# Create the PySpark DataFrame
products = spark.createDataFrame(data, schema=columns)

# Display the PySpark DataFrame
products.show()


# COMMAND ----------

products.filter((col('low_fats')=='Y')&(col('recyclable')=='Y')).show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ###183. Customers Who Never Order

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("CustomersAndOrders").getOrCreate()

# Define the customers data as a list of tuples
customers_data = [
    (1, "Joe"),
    (2, "Henry"),
    (3, "Sam"),
    (4, "Max")
]

# Define the orders data as a list of tuples
orders_data = [
    (1, 3),
    (2, 1)
]

# Define column names for each DataFrame
customers_columns = ["id", "name"]
orders_columns = ["id", "customerId"]

# Create the PySpark DataFrames
customers = spark.createDataFrame(customers_data, schema=customers_columns)
orders = spark.createDataFrame(orders_data, schema=orders_columns)

# Display the DataFrames
print("Customers DataFrame:")
customers.show()

print("\nOrders DataFrame:")
orders.show()


# COMMAND ----------

customers.join(orders,customers.id==orders.customerId,how='left_anti')\
    .selectExpr("name as Customers")\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###1148. Article Views I

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark session
spark = SparkSession.builder.appName("CreateViews").getOrCreate()

# Define the schema
schema = StructType([
    StructField("article_id", IntegerType(), True),
    StructField("author_id", IntegerType(), True),
    StructField("viewer_id", IntegerType(), True),
    StructField("view_date", StringType(), True)
])

# Define the data
data = [
    (1, 3, 5, '2019-08-01'),
    (1, 3, 6, '2019-08-02'),
    (2, 7, 7, '2019-08-01'),
    (2, 7, 6, '2019-08-02'),
    (4, 7, 1, '2019-07-22'),
    (3, 4, 4, '2019-07-21'),
    (3, 4, 4, '2019-07-21')
]

# Create the DataFrame
views = spark.createDataFrame(data, schema)

views.show()


# COMMAND ----------

views.filter(col('author_id')==col('viewer_id'))\
    .dropDuplicates(subset=['author_id'])\
    .selectExpr('author_id as id')\
    .orderBy('id')\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###1683. Invalid Tweets

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark session
spark = SparkSession.builder.appName("CreateTweets").getOrCreate()

# Define the schema
schema = StructType([
    StructField("tweet_id", IntegerType(), True),
    StructField("content", StringType(), True)
])

# Define the data
data = [
    (1, "Let us Code"),
    (2, "More than fifteen chars are here!")
]

# Create the DataFrame
tweets = spark.createDataFrame(data, schema)

tweets.show()


# COMMAND ----------

tweets.filter(length(col('content'))>15)\
    .select('tweet_id')\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###1873. Calculate Special Bonus

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark session
spark = SparkSession.builder.appName("CreateEmployees").getOrCreate()

# Define the schema
schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True)
])

# Define the data
data = [
    (2, "Meir", 3000),
    (3, "Michael", 3800),
    (7, "Addilyn", 7400),
    (8, "Juan", 6100),
    (9, "Kannon", 7700)
]

# Create the DataFrame
employees = spark.createDataFrame(data, schema)

employees.show()


# COMMAND ----------

employees.withColumn('bonus',\
    when((~col('name').startswith('M'))&(col('employee_id')%2==1),col('salary')).otherwise(0)\
)\
.select('employee_id','bonus')\
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###1667. Fix Names in a Table

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark session
spark = SparkSession.builder.appName("CreateUsers").getOrCreate()

# Define the schema
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("name", StringType(), True)
])

# Define the data
data = [
    (1, "aLice"),
    (2, "bOB")
]

# Create the DataFrame
users = spark.createDataFrame(data, schema)

# Show the DataFrame
users.show()


# COMMAND ----------

users.select('user_id', initcap(col('name')).alias('name')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###1517. Find Users With Valid E-Mails

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark Session (already available in Databricks notebooks)
spark = SparkSession.builder.appName("Users DataFrame").getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("mail", StringType(), True)
])

# Create the data
data = [
    (1, "Winston", "winston@leetcode.com"),
    (2, "Jonathan", "jonathanisgreat"),
    (3, "Annabelle", "bella-@leetcode.com"),
    (4, "Sally", "sally.come@leetcode.com"),
    (5, "Marwan", "quarz#2020@leetcode.com"),
    (6, "David", "david69@gmail.com"),
    (7, "Shapiro", ".shapo@leetcode.com")
]

# Create the DataFrame
users = spark.createDataFrame(data, schema)

# Show the DataFrame
users.show()


# COMMAND ----------

users.filter(col('mail').rlike('^[a-zA-Z][a-zA-Z0-9_\.\-]*@leetcode.com$')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###1527. Patients With a Condition

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark Session (already available in Databricks notebooks)
spark = SparkSession.builder.appName("Patient DataFrame").getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("patient_id", IntegerType(), True),
    StructField("patient_name", StringType(), True),
    StructField("conditions", StringType(), True)
])

# Create the data
data = [
    (1, "Daniel", "YFEV COUGH"),
    (2, "Alice", ""),
    (3, "Bob", "DIAB100 MYOP"),
    (4, "George", "ACNE DIAB100"),
    (5, "Alain", "DIAB201")
]

# Create the DataFrame
patient = spark.createDataFrame(data, schema)

# Show the DataFrame
patient.show()


# COMMAND ----------

patient.filter(col('conditions').rlike('^DIAB1') | col('conditions').rlike('.* DIAB1'))\
    .show()

# COMMAND ----------

patient.filter(col('conditions').like('DIAB1%') | col('conditions').like('% DIAB1%'))\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###2738. Count Occurrences in Text

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark Session (already available in Databricks notebooks)
spark = SparkSession.builder.appName("Files DataFrame").getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("content", StringType(), True)
])

# Create the data
data = [
    ("draft1.txt", "The stock exchange predicts a bull market which would make many investors happy."),
    ("draft2.txt", "The stock exchange predicts a bull market which would make many investors happy, "
                   "but analysts warn of possibility of too much optimism and that in fact we are "
                   "awaiting a bear market."),
    ("draft3.txt", "The stock exchange predicts a bull market which would make many investors happy, "
                   "but analysts warn of possibility of too much optimism and that in fact we are "
                   "awaiting a bear market. As always predicting the future market is an uncertain "
                   "game and all investors should follow their instincts and best practices.")
]

# Create the DataFrame
files = spark.createDataFrame(data, schema)

# Show the DataFrame
files.show(truncate=False)


# COMMAND ----------

bull = files.filter(
    lower(col('content')).like('% bull %')
).count()

bear = files.filter(
    lower(col('content')).like('% bear %')
).count()

data = [
    ('bull',bull),
    ('bear',bear)
]
display(data)

# COMMAND ----------

spark.createDataFrame(data,['word', 'count']).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###177. Nth Highest Salary

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

# Initialize Spark Session (already available in Databricks notebooks)
spark = SparkSession.builder.appName("Employee DataFrame").getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("salary", IntegerType(), True)
])

# Create the data
data = [
    (1, 100),
    (2, 200),
    (3, 300)
]

# Create the DataFrame
employee = spark.createDataFrame(data, schema)

# Show the DataFrame
employee.show()


# COMMAND ----------

n = 3

if (n < 1) | (n > employee.dropDuplicates(subset=['salary']).count()):
    df = spark.createDataFrame(
            [(None,)],
            StructType([
                StructField('id',IntegerType(),True)
            ])
        )
    display(df)
else:
    windows_spec = Window.orderBy(col('salary').asc())
    employee.withColumn('rank',row_number().over(windows_spec)).filter(col('rank')==n)\
        .selectExpr(f"salary as getNthHighestSalary_{n}")\
        .show()



# COMMAND ----------

# MAGIC %md
# MAGIC ###176. Second Highest Salary

# COMMAND ----------

employee.show()

# COMMAND ----------

n = 2
window_sal = Window.orderBy('salary')
employee.dropDuplicates(['salary'])\
    .withColumn('rank',row_number().over(window_sal))\
    .filter(col('rank')==n)\
    .selectExpr('salary as SecondHighestSalary')\
    .show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

# Initialize Spark Session (already available in Databricks notebooks)
spark = SparkSession.builder.appName("Employee DataFrame").getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("salary", IntegerType(), True)
])

# Create the data
data = [
    (1, 100)
]

# Create the DataFrame
employee = spark.createDataFrame(data, schema)

# Show the DataFrame
employee.show()


# COMMAND ----------

n = 2
employee = employee.dropDuplicates(['salary'])
if employee.count() < 2:
    df = spark.createDataFrame(
        [(None,)],
        StructType([
            StructField('SecondHighestSalary',IntegerType(),True)
        ])
    )
    display(df)
else:
    window_sal = Window.orderBy('salary')
    employee.withColumn('rank',row_number().over(window_sal))\
        .filter(col('rank')==n)\
        .selectExpr('salary as SecondHighestSalary')\
        .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###184. Department Highest Salary

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("DataFrameExample").getOrCreate()

# Create the Employee DataFrame
employee = spark.createDataFrame(
    [
        (1, "Joe", 70000, 1),
        (2, "Jim", 90000, 1),
        (3, "Henry", 80000, 2),
        (4, "Sam", 60000, 2),
        (5, "Max", 90000, 1),
    ],
    ["id", "name", "salary", "departmentId"]
)

# Create the Department DataFrame
department = spark.createDataFrame(
    [
        (1, "IT"),
        (2, "Sales"),
    ],
    ["id", "name"]
)

# Show the DataFrames
employee.show()
department.show()


# COMMAND ----------

window_spec = Window.partitionBy(col("d.id")).orderBy(col('salary').desc())
department.alias('d').join(employee.alias('e'), department.id == employee.departmentId, how='left')\
    .withColumn('rank',rank().over(window_spec))\
    .filter(col('rank')==1)\
    .selectExpr('d.name as Department','e.name as Employee','Salary')\
    .show()
    

# COMMAND ----------

# MAGIC %md
# MAGIC ###178. Rank Scores

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("ScoresDataFrame").getOrCreate()

# Create the Scores DataFrame
scores = spark.createDataFrame(
    [
        (1, 3.50),
        (2, 3.65),
        (3, 4.00),
        (4, 3.85),
        (5, 4.00),
        (6, 3.65),
    ],
    ["id", "score"]
)

# Show the Scores DataFrame
scores.show()


# COMMAND ----------

window_spec = Window.orderBy(col('score').desc())
scores.withColumn('rank',dense_rank().over(window=window_spec))\
    .selectExpr('score','rank')\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###196. Delete Duplicate Emails

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("PersonDataFrame").getOrCreate()

# Create the Person DataFrame
person = spark.createDataFrame(
    [
        (1, "john@example.com"),
        (2, "bob@example.com"),
        (3, "john@example.com"),
    ],
    ["id", "email"]
)

# Show the Person DataFrame
person.show()


# COMMAND ----------

person.dropDuplicates(subset=['email']).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###1795. Rearrange Products Table

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("ProductsDataFrame").getOrCreate()

# Create the Products DataFrame
products = spark.createDataFrame(
    [
        (0, 95, 100, 105),
        (1, 70, None, 80),
    ],
    ["product_id", "store1", "store2", "store3"]
)

# Show the Products DataFrame
products.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ###2082. The Number of Rich Customers

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("StoreDataFrame").getOrCreate()

# Create the Store DataFrame
store = spark.createDataFrame(
    [
        (6, 1, 549),
        (8, 1, 834),
        (4, 2, 394),
        (11, 3, 657),
        (13, 3, 257),
    ],
    ["bill_id", "customer_id", "amount"]
)

# Show the Store DataFrame
store.show()


# COMMAND ----------

cnt = store.filter(col('amount')>500).select('customer_id').distinct().count()
df = spark.createDataFrame([(cnt,)],['rich_count'])
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###1173. Immediate Food Delivery I

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("DeliveryDataFrame").getOrCreate()

# Create the Delivery DataFrame
delivery = spark.createDataFrame(
    [
        (1, 1, "2019-08-01", "2019-08-02"),
        (2, 5, "2019-08-02", "2019-08-02"),
        (3, 1, "2019-08-11", "2019-08-11"),
        (4, 3, "2019-08-24", "2019-08-26"),
        (5, 4, "2019-08-21", "2019-08-22"),
        (6, 2, "2019-08-11", "2019-08-13"),
    ],
    ["delivery_id", "customer_id", "order_date", "customer_pref_delivery_date"]
)

# Show the Delivery DataFrame
delivery.show()


# COMMAND ----------

total_order = delivery.count()
immediate_order = delivery.filter(col('order_date')==col('customer_pref_delivery_date')).count()
per = immediate_order/total_order

df = spark.createDataFrame([(per,)],['immediate_percentage'])
df.select(round(col('immediate_percentage'),2).alias('immediate_percentage')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###1907. Count Salary Categories

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("AccountsDataFrame").getOrCreate()

# Create the Accounts DataFrame
accounts = spark.createDataFrame(
    [
        (3, 108939),
        (2, 12747),
        (8, 87709),
        (6, 91796),
    ],
    ["account_id", "income"]
)

# Show the Accounts DataFrame
accounts.show()


# COMMAND ----------

low = accounts.filter(col('income')<20000).count()
average = accounts.filter((col('income') > 20000) & (col('income') < 50000)).count()
high = accounts.filter(col('income')>50000).count()
print(low,average,high)


# COMMAND ----------

spark.createDataFrame(
    [
        ('Low Salary', low),
        ('Average Salary', average),
        ('high Salary', high),
    ],
    ['category','accounts_count']
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###1322. Ads Performance

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("AdsDataFrame").getOrCreate()

# Create the Ads DataFrame
ads = spark.createDataFrame(
    [
        (1, 1, "Clicked"),
        (2, 2, "Clicked"),
        (3, 3, "Viewed"),
        (5, 5, "Ignored"),
        (1, 7, "Ignored"),
        (2, 7, "Viewed"),
        (3, 5, "Clicked"),
        (1, 4, "Viewed"),
        (2, 11, "Viewed"),
        (1, 2, "Clicked"),
    ],
    ["ad_id", "user_id", "action"]
)

# Show the Ads DataFrame
ads.show()


# COMMAND ----------

clicks = ads.groupBy('ad_id').agg(sum(when(col('action')=='Clicked',1).otherwise(0)).alias('clicks'),
                                  sum(when((col('action')=='Clicked')|(col('action')=='Viewed'),1).otherwise(0)).alias('total'))

clicks.show()

clicks.withColumn('ctr',(col('clicks')/col('total'))*100).fillna(0)\
    .select('ad_id',round('ctr',2).alias('ctr'))\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###1741. Find Total Time Spent by Each Employee

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("EmployeesDataFrame").getOrCreate()

# Create the Employees DataFrame
employees = spark.createDataFrame(
    [
        (1, "2020-11-28", 4, 32),
        (1, "2020-11-28", 55, 200),
        (1, "2020-12-03", 1, 42),
        (2, "2020-11-28", 3, 33),
        (2, "2020-12-09", 47, 74),
    ],
    ["emp_id", "event_day", "in_time", "out_time"]
)

# Show the Employees DataFrame
employees.show()


# COMMAND ----------

employees.groupBy(col('event_day').alias('day'),'emp_id')\
    .agg(sum(col('out_time')-col('in_time'))\
    .alias('total_time'))\
    .orderBy(['day'])\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###511. Game Play Analysis I

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("ActivityDataFrame").getOrCreate()

# Create the Activity DataFrame
activity = spark.createDataFrame(
    [
        (1, 2, "2016-03-01", 5),
        (1, 2, "2016-05-02", 6),
        (2, 3, "2017-06-25", 1),
        (3, 1, "2016-03-02", 0),
        (3, 4, "2018-07-03", 5),
    ],
    ["player_id", "device_id", "event_date", "games_played"]
)

# activity = activity.withColumn('event_date',col('event_date').cast(DateType()))

# Show the Activity DataFrame
activity.show()


# COMMAND ----------

activity.groupBy("player_id").agg(min('event_date').alias('first_login')).show()


# COMMAND ----------

# MAGIC %md
# MAGIC ###2356. Number of Unique Subjects Taught by Each Teacher

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("TeacherDataFrame").getOrCreate()

# Create the Teacher DataFrame
teacher = spark.createDataFrame(
    [
        (1, 2, 3),
        (1, 2, 4),
        (1, 3, 3),
        (2, 1, 1),
        (2, 2, 1),
        (2, 3, 1),
        (2, 4, 1),
    ],
    ["teacher_id", "subject_id", "dept_id"]
)

# Show the Teacher DataFrame
teacher.show()


# COMMAND ----------

teacher.groupBy('teacher_id').agg(countDistinct('subject_id').alias('cnt')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###596. Classes More Than 5 Students

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("CoursesDataFrame").getOrCreate()

# Create the Courses DataFrame
courses = spark.createDataFrame(
    [
        ("A", "Math"),
        ("B", "English"),
        ("C", "Math"),
        ("D", "Biology"),
        ("E", "Math"),
        ("F", "Computer"),
        ("G", "Math"),
        ("H", "Math"),
        ("I", "Math"),
    ],
    ["student", "class"]
)

# Show the Courses DataFrame
courses.show()


# COMMAND ----------

courses.groupBy('class').count().filter(col('count')>5).select('class').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###586. Customer Placing the Largest Number of Orders

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("OrdersDataFrame").getOrCreate()

# Create the Orders DataFrame
orders = spark.createDataFrame(
    [
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 3),
    ],
    ["order_number", "customer_number"]
)

# Show the Orders DataFrame
orders.show()


# COMMAND ----------

orders.groupBy('customer_number').count()\
    .orderBy(col('count').desc())\
    .limit(1)\
    .select('customer_number')\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###1484. Group Sold Products By The Date

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("ActivitiesDataFrame").getOrCreate()

# Create the Activities DataFrame
activities = spark.createDataFrame(
    [
        ("2020-05-30", "Headphone"),
        ("2020-06-01", "Pencil"),
        ("2020-06-02", "Mask"),
        ("2020-05-30", "Basketball"),
        ("2020-06-01", "Bible"),
        ("2020-06-02", "Mask"),
        ("2020-05-30", "T-Shirt"),
    ],
    ["sell_date", "product"]
)

# Show the Activities DataFrame
activities.show()


# COMMAND ----------

activities.dropDuplicates()\
    .orderBy('sell_date','product')\
    .groupBy('sell_date').agg(
    count('product').alias('num_sold'),
    concat_ws(', ',collect_list('product')).alias('products')
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###1693. Daily Leads and Partners

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("DailySalesDataFrame").getOrCreate()

# Create the DailySales DataFrame
daily_sales = spark.createDataFrame(
    [
        ("2020-12-8", "toyota", 0, 1),
        ("2020-12-8", "toyota", 1, 0),
        ("2020-12-8", "toyota", 1, 2),
        ("2020-12-7", "toyota", 0, 2),
        ("2020-12-7", "toyota", 0, 1),
        ("2020-12-8", "honda", 1, 2),
        ("2020-12-8", "honda", 2, 1),
        ("2020-12-7", "honda", 0, 1),
        ("2020-12-7", "honda", 1, 2),
        ("2020-12-7", "honda", 2, 1),
    ],
    ["date_id", "make_name", "lead_id", "partner_id"]
)

# Show the DailySales DataFrame
daily_sales.show()


# COMMAND ----------

daily_sales.groupBy('date_id','make_name').agg(
    countDistinct('lead_id').alias('unique_leads'),
    countDistinct('partner_id').alias('unique_partners')
    ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###1050. Actors and Directors Who Cooperated At Least Three Times

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("ActorDirector").getOrCreate()

# Create the data for the DataFrame
data = [
    (1, 1, 0),
    (1, 1, 1),
    (1, 1, 2),
    (1, 2, 3),
    (1, 2, 4),
    (2, 1, 5),
    (2, 1, 6)
]

# Define the schema
schema = ["actor_id", "director_id", "timestamp"]

# Create the Spark DataFrame
actor_director = spark.createDataFrame(data, schema)

# Show the DataFrame
actor_director.show()


# COMMAND ----------

actor_director.groupBy(['actor_id','director_id']).count().filter(col('count')>2)\
    .select('actor_id','director_id')\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###1378. Replace Employee ID With The Unique Identifier

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Employees").getOrCreate()

# Create the data for the Employees DataFrame
employees_data = [
    (1, "Alice"),
    (7, "Bob"),
    (11, "Meir"),
    (90, "Winston"),
    (3, "Jonathan")
]

# Define the schema for Employees DataFrame
employees_schema = ["id", "name"]

# Create the Employees DataFrame
employees = spark.createDataFrame(employees_data, employees_schema)

# Show the Employees DataFrame
employees.show()

# Create the data for the EmployeeUNI DataFrame
employee_uni_data = [
    (3, 1),
    (11, 2),
    (90, 3)
]

# Define the schema for EmployeeUNI DataFrame
employee_uni_schema = ["id", "unique_id"]

# Create the EmployeeUNI DataFrame
employee_uni = spark.createDataFrame(employee_uni_data, employee_uni_schema)

# Show the EmployeeUNI DataFrame
employee_uni.show()


# COMMAND ----------

employees.alias('e').join(employee_uni.alias('eu'), col('e.id') == col('eu.id'), how='left' )\
    .select('unique_id','name')\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###1280. Students and Examinations

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("SchoolData").getOrCreate()

# Create the data for the Students DataFrame
students_data = [
    (1, "Alice"),
    (2, "Bob"),
    (13, "John"),
    (6, "Alex")
]

# Define the schema for Students DataFrame
students_schema = ["student_id", "student_name"]

# Create the Students DataFrame
students = spark.createDataFrame(students_data, students_schema)

# Show the Students DataFrame
students.show()

# Create the data for the Subjects DataFrame
subjects_data = [
    ("Math",),
    ("Physics",),
    ("Programming",)
]

# Define the schema for Subjects DataFrame
subjects_schema = ["subject_name"]

# Create the Subjects DataFrame
subjects = spark.createDataFrame(subjects_data, subjects_schema)

# Show the Subjects DataFrame
subjects.show()

# Create the data for the Examinations DataFrame
examinations_data = [
    (1, "Math"),
    (1, "Physics"),
    (1, "Programming"),
    (2, "Programming"),
    (1, "Physics"),
    (1, "Math"),
    (13, "Math"),
    (13, "Programming"),
    (13, "Physics"),
    (2, "Math"),
    (1, "Math")
]

# Define the schema for Examinations DataFrame
examinations_schema = ["student_id", "subject_name"]

# Create the Examinations DataFrame
examinations = spark.createDataFrame(examinations_data, examinations_schema)

# Show the Examinations DataFrame
examinations.show()


# COMMAND ----------

students.crossJoin(subjects).alias('s')\
    .join(examinations.alias('e'), (col('s.student_id')==col('e.student_id'))&(col('s.subject_name')==col('e.subject_name')), how='left')\
    .groupBy(col('s.student_id'),'student_name','s.subject_name').agg(count(col('e.subject_name')).alias('attended_exams'))\
    .orderBy('student_id','subject_name')\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###570. Managers with at Least 5 Direct Reports

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("EmployeeData").getOrCreate()

# Create the data for the Employee DataFrame
employee_data = [
    (101, "John", "A", None),
    (102, "Dan", "A", 101),
    (103, "James", "A", 101),
    (104, "Amy", "A", 101),
    (105, "Anne", "A", 101),
    (106, "Ron", "B", 101)
]

# Define the schema for Employee DataFrame
employee_schema = ["id", "name", "department", "managerId"]

# Create the Employee DataFrame
employee = spark.createDataFrame(employee_data, employee_schema)

# Show the Employee DataFrame
employee.show()


# COMMAND ----------

employee.alias("e1").join(employee.alias("e2"), col('e1.id')==col('e2.managerId'),how='inner')\
    .groupBy('e1.id','e1.name').count()\
    .filter(col('count')>4)\
    .select('name')\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###607. Sales Person

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("SalesData").getOrCreate()

# Create the data for the SalesPerson DataFrame
sales_person_data = [
    (1, "John", 100000, 6, "4/1/2006"),
    (2, "Amy", 12000, 5, "5/1/2010"),
    (3, "Mark", 65000, 12, "12/25/2008"),
    (4, "Pam", 25000, 25, "1/1/2005"),
    (5, "Alex", 5000, 10, "2/3/2007")
]

# Define the schema for SalesPerson DataFrame
sales_person_schema = ["sales_id", "name", "salary", "commission_rate", "hire_date"]

# Create the SalesPerson DataFrame
sales_person = spark.createDataFrame(sales_person_data, sales_person_schema)

# Show the SalesPerson DataFrame
sales_person.show()

# Create the data for the Company DataFrame
company_data = [
    (1, "RED", "Boston"),
    (2, "ORANGE", "New York"),
    (3, "YELLOW", "Boston"),
    (4, "GREEN", "Austin")
]

# Define the schema for Company DataFrame
company_schema = ["com_id", "name", "city"]

# Create the Company DataFrame
company = spark.createDataFrame(company_data, company_schema)

# Show the Company DataFrame
company.show()

# Create the data for the Orders DataFrame
orders_data = [
    (1, "1/1/2014", 3, 4, 10000),
    (2, "2/1/2014", 4, 5, 5000),
    (3, "3/1/2014", 1, 1, 50000),
    (4, "4/1/2014", 1, 4, 25000)
]

# Define the schema for Orders DataFrame
orders_schema = ["order_id", "order_date", "com_id", "sales_id", "amount"]

# Create the Orders DataFrame
orders = spark.createDataFrame(orders_data, orders_schema)

# Show the Orders DataFrame
orders.show()


# COMMAND ----------

red_companies = company.filter(col('name')=='RED').select('com_id')

red_orders = orders.join(red_companies.alias('rc'), orders.com_id == col('rc.com_id'), how='inner').select('sales_id')

sales_person.alias('sp').join(red_orders.alias('ro'), col('sp.sales_id')==col('ro.sales_id'), how='left_anti')\
    .select('name')\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###2041 - Accepted Candidates From the Interviews

# COMMAND ----------

from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("CreateDataFrames").getOrCreate()

# Data for Candidates table
candidates_data = [
    (11, "Atticus", 1, 101),
    (9, "Ruben", 6, 104),
    (6, "Aliza", 10, 109),
    (8, "Alfredo", 0, 107)
]

# Define schema for Candidates table
candidates_columns = ["candidate_id", "name", "years_of_exp", "interview_id"]

# Create Candidates DataFrame
candidates = spark.createDataFrame(candidates_data, schema=candidates_columns)

# Data for Rounds table
rounds_data = [
    (109, 3, 4),
    (101, 2, 8),
    (109, 4, 1),
    (107, 1, 3),
    (104, 3, 6),
    (109, 1, 4),
    (104, 4, 7),
    (104, 1, 2),
    (109, 2, 1),
    (104, 2, 7),
    (107, 2, 3),
    (101, 1, 8)
]

# Define schema for Rounds table
rounds_columns = ["interview_id", "round_id", "score"]

# Create Rounds DataFrame
rounds = spark.createDataFrame(rounds_data, schema=rounds_columns)

# Show the data
print("Candidates Table:")
candidates.show()

print("Rounds Table:")
rounds.show()


# COMMAND ----------

candidates.filter(col('years_of_exp')>1).alias('c').join(
    rounds.groupBy('interview_id').agg(sum('score').alias('score')).alias('r'),
    col('r.interview_id')==col('c.interview_id'),
    how='inner'
    )\
    .filter(col('score')>15)\
    .select('candidate_id')\
    .show()
