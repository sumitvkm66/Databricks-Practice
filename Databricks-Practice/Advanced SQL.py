# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ###1821. Find Customers With Positive Revenue this Year

# COMMAND ----------

from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("CustomersDataFrame").getOrCreate()

# Data for Customers table
customers_data = [
    (1, 2018, 50),
    (1, 2021, 30),
    (1, 2020, 70),
    (2, 2021, -50),
    (3, 2018, 10),
    (3, 2016, 50),
    (4, 2021, 20)
]

# Define schema for Customers table
customers_columns = ["customer_id", "year", "revenue"]

# Create Customers DataFrame
customers = spark.createDataFrame(customers_data, schema=customers_columns)

# Show the data
customers.show()


# COMMAND ----------

customers.filter(
    (col('year') == 2021) &
    (col('revenue')>0)
)\
.select('customer_id')\
.show()

# COMMAND ----------

customers.createOrReplaceTempView('customers')

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id from customers where year = 2021 and revenue > 0

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

customers.join(orders, customers.id==orders.customerId, how='left_anti').select('name').show()

# COMMAND ----------

customers.createOrReplaceTempView('customers')
orders.createOrReplaceTempView('orders')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select name from customers where id not in (select customerId from orders);
# MAGIC select name from customers c
# MAGIC left join orders o on c.id = o.customerId
# MAGIC where o.customerId is null;

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

employees.withColumn('bonus',
                     when((~col('name').startswith('M'))&(col('employee_id')%2==1),col('salary')).otherwise(0)
                     )\
                    .orderBy('employee_id')\
                    .show()

# COMMAND ----------

employees.createOrReplaceTempView('employees')

# COMMAND ----------

# MAGIC %sql
# MAGIC select employee_id,
# MAGIC   case when name not like 'M%' and employee_id%2=1 then salary else 0 end as bonus
# MAGIC  from employees
# MAGIC  order by employee_id

# COMMAND ----------

# MAGIC %md
# MAGIC ###1398. Customers Who Bought Products A and B but Not C

# COMMAND ----------

from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("CreateDataFrames").getOrCreate()

# Data for Customers table
customers_data = [
    (1, "Daniel"),
    (2, "Diana"),
    (3, "Elizabeth"),
    (4, "Jhon")
]

# Define schema for Customers table
customers_columns = ["customer_id", "customer_name"]

# Create Customers DataFrame
customers = spark.createDataFrame(customers_data, schema=customers_columns)

# Data for Orders table
orders_data = [
    (10, 1, "A"),
    (20, 1, "B"),
    (30, 1, "D"),
    (40, 1, "C"),
    (50, 2, "A"),
    (60, 3, "A"),
    (70, 3, "B"),
    (80, 3, "D"),
    (90, 4, "C")
]

# Define schema for Orders table
orders_columns = ["order_id", "customer_id", "product_name"]

# Create Orders DataFrame
orders = spark.createDataFrame(orders_data, schema=orders_columns)

# Show the data
print("Customers Table:")
customers.show()

print("Orders Table:")
orders.show()


# COMMAND ----------

orders.alias('o').join(customers.alias('c'), customers.customer_id == orders.customer_id, how='left')\
    .groupBy('o.customer_id','customer_name').agg(
        sum(when(col('product_name')=='A',1).otherwise(0)).alias('total_A'),
        sum(when(col('product_name')=='B',1).otherwise(0)).alias('total_B'),
        sum(when(col('product_name')=='C',1).otherwise(0)).alias('total_C'),
    ).filter((col('total_A')>0)&(col('total_B')>0)&(col('total_C')==0))\
        .select('customer_id','customer_name')\
        .show()

# COMMAND ----------

a_orders = orders.filter(col('product_name')=='A').select('customer_id')
b_orders = orders.filter(col('product_name')=='B').select('customer_id')
c_orders = orders.filter(col('product_name')=='C').select('customer_id')

customers.alias('cc').join(a_orders.alias('a'), col('a.customer_id')==col('cc.customer_id'),how='inner')\
    .select('cc.customer_id','customer_name').alias('cc')\
    .join(b_orders.alias('b'), col('b.customer_id')==col('cc.customer_id'),how='inner')\
    .select('cc.customer_id','customer_name').alias('cc')\
    .join(c_orders.alias('c'), col('c.customer_id')==col('cc.customer_id'), how='left_anti')\
    .show()

# COMMAND ----------

customers.createOrReplaceTempView('customers')
orders.createOrReplaceTempView('orders')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select c.customer_id, customer_name
# MAGIC from orders o 
# MAGIC left join customers c on c.customer_id = o.customer_id
# MAGIC group by all
# MAGIC having sum(case when product_name='A' then 1 else 0 end)>0 and 
# MAGIC        sum(case when product_name='B' then 1 else 0 end)>0 and 
# MAGIC        sum(case when product_name='C' then 1 else 0 end)=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select cc.customer_id, customer_name
# MAGIC from customers cc
# MAGIC join orders a on cc.customer_id = a.customer_id and a.product_name = 'A'
# MAGIC join orders b on cc.customer_id = b.customer_id and b.product_name = 'B'
# MAGIC left join orders c on c.customer_id = cc.customer_id and c.product_name = 'C'
# MAGIC where c.customer_id is null
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###1112. Highest Grade For Each Student

# COMMAND ----------

from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("CreateEnrollmentsResult").getOrCreate()

# Data for Enrollments table
enrollments_data = [
    (2, 2, 95),
    (2, 3, 95),
    (1, 1, 90),
    (1, 2, 99),
    (3, 1, 80),
    (3, 2, 75),
    (3, 3, 82)
]

# Define schema for Enrollments table
enrollments_columns = ["student_id", "course_id", "grade"]

# Create Enrollments DataFrame
enrollments = spark.createDataFrame(enrollments_data, schema=enrollments_columns)


print("Enrollments Table:")
enrollments.show()

# COMMAND ----------

from pyspark.sql.window import Window

window_spec = Window.partitionBy('student_id').orderBy(col('grade').desc(), col('course_id'))

enrollments.withColumn('rank',row_number().over(window=window_spec)).filter(col('rank')==1)\
    .select('student_id','course_id','grade')\
    .orderBy('student_id')\
    .show()

# COMMAND ----------

enrollments.createOrReplaceTempView('enrollments')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from enrollments
# MAGIC qualify row_number() over (partition by student_id order by grade desc, course_id) = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ###175. Combine Two Tables

# COMMAND ----------

from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("CreatePersonAddress").getOrCreate()

# Data for Person table
person_data = [
    (1, "Wang", "Allen"),
    (2, "Alice", "Bob")
]

# Define schema for Person table
person_columns = ["personId", "lastName", "firstName"]

# Create Person DataFrame
person = spark.createDataFrame(person_data, schema=person_columns)

# Data for Address table
address_data = [
    (1, 2, "New York City", "New York"),
    (2, 3, "Leetcode", "California")
]

# Define schema for Address table
address_columns = ["addressId", "personId", "city", "state"]

# Create Address DataFrame
address = spark.createDataFrame(address_data, schema=address_columns)

# Show the data
print("Person Table:")
person.show()

print("Address Table:")
address.show()


# COMMAND ----------

person.join(address, person.personId == address.personId, how='left')\
    .select('firstName','lastName','city','state')\
    .show()

# COMMAND ----------

person.createOrReplaceTempView('person')
address.createOrReplaceTempView('address')

# COMMAND ----------

# MAGIC %sql
# MAGIC select firstName, lastName, city, state
# MAGIC from person p
# MAGIC left join address a using(personId)

# COMMAND ----------

# MAGIC %md
# MAGIC ###1607. Sellers With No Sales

# COMMAND ----------

from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("CreateDataFrames").getOrCreate()

# Data for Customer table
customer_data = [
    (101, "Alice"),
    (102, "Bob"),
    (103, "Charlie")
]

# Define schema for Customer table
customer_columns = ["customer_id", "customer_name"]

# Create Customer DataFrame
customer = spark.createDataFrame(customer_data, schema=customer_columns)

# Data for Orders table
orders_data = [
    (1, "2020-03-01", 1500, 101, 1),
    (2, "2020-05-25", 2400, 102, 2),
    (3, "2019-05-25", 800, 101, 3),
    (4, "2020-09-13", 1000, 103, 2),
    (5, "2019-02-11", 700, 101, 2)
]

# Define schema for Orders table
orders_columns = ["order_id", "sale_date", "order_cost", "customer_id", "seller_id"]

# Create Orders DataFrame
orders = spark.createDataFrame(orders_data, schema=orders_columns)

# Data for Seller table
seller_data = [
    (1, "Daniel"),
    (2, "Elizabeth"),
    (3, "Frank")
]

# Define schema for Seller table
seller_columns = ["seller_id", "seller_name"]

# Create Seller DataFrame
seller = spark.createDataFrame(seller_data, schema=seller_columns)

# Show the data
print("Customer Table:")
customer.show()

print("Orders Table:")
orders.show()

print("Seller Table:")
seller.show()


# COMMAND ----------

orders_2020 = orders.filter(year(col('sale_date'))==2020).select('seller_id')

seller.join(orders_2020.alias('o'), seller.seller_id==col('o.seller_id'), how='left_anti').select('seller_name').show()

# COMMAND ----------

orders.createOrReplaceTempView('orders')
seller.createOrReplaceTempView('seller')

# COMMAND ----------

# MAGIC %sql
# MAGIC select seller_name
# MAGIC from seller s
# MAGIC left join orders o on o.seller_id = s.seller_id and year(sale_date) = 2020
# MAGIC where o.seller_id is null

# COMMAND ----------

# MAGIC %md
# MAGIC ###1407. Top Travellers

# COMMAND ----------

from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("CreateUsersRides").getOrCreate()

# Data for Users table
users_data = [
    (1, "Alice"),
    (2, "Bob"),
    (3, "Alex"),
    (4, "Donald"),
    (7, "Lee"),
    (13, "Jonathan"),
    (19, "Elvis")
]

# Define schema for Users table
users_columns = ["id", "name"]

# Create Users DataFrame
users = spark.createDataFrame(users_data, schema=users_columns)

# Data for Rides table
rides_data = [
    (1, 1, 120),
    (2, 2, 317),
    (3, 3, 222),
    (4, 7, 100),
    (5, 13, 312),
    (6, 19, 50),
    (7, 7, 120),
    (8, 19, 400),
    (9, 7, 230)
]

# Define schema for Rides table
rides_columns = ["id", "user_id", "distance"]

# Create Rides DataFrame
rides = spark.createDataFrame(rides_data, schema=rides_columns)

# Show the data
print("Users Table:")
users.show()

print("Rides Table:")
rides.show()


# COMMAND ----------

users.join(rides, users.id==rides.user_id, how='left')\
    .groupBy('user_id','name').agg(sum('distance').alias('travelled_distance'))\
    .fillna(0)\
    .select('name','travelled_distance')\
    .orderBy(col('travelled_distance').desc(), 'name')\
    .show()

# COMMAND ----------

users.createOrReplaceTempView('users')
rides.createOrReplaceTempView('rides')

# COMMAND ----------

# MAGIC %sql
# MAGIC select name, ifnull(sum(distance),0) travelled_distance
# MAGIC from users u
# MAGIC left join rides r on r.user_id = u.id
# MAGIC group by user_id, name
# MAGIC order by 2 desc, 1

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

sales_person.alias('sa').join(red_orders.alias('ra'), col('ra.sales_id')==col('sa.sales_id'), how='left_anti')\
    .select('name')\
    .show()

# COMMAND ----------

sales_person.createOrReplaceTempView('sales_person')
company.createOrReplaceTempView('company')
orders.createOrReplaceTempView('orders')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select name
# MAGIC from sales_person sp
# MAGIC left join(
# MAGIC select sales_id
# MAGIC from orders where com_id = (
# MAGIC   select com_id from company where name = 'RED'
# MAGIC )) rs on rs.sales_id = sp.sales_id
# MAGIC where rs.sales_id is null

# COMMAND ----------

# MAGIC %md
# MAGIC ###1440. Evaluate Boolean Expression

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create a SparkSession
spark = SparkSession.builder.appName("VariablesExpressions").getOrCreate()

# Define data for Variables
variables_data = [Row(name="x", value=66), Row(name="y", value=77)]
variables = spark.createDataFrame(variables_data)

# Define data for Expressions
expressions_data = [
    Row(left_operand="x", operator=">", right_operand="y"),
    Row(left_operand="x", operator="<", right_operand="y"),
    Row(left_operand="x", operator="=", right_operand="y"),
    Row(left_operand="y", operator=">", right_operand="x"),
    Row(left_operand="y", operator="<", right_operand="x"),
    Row(left_operand="x", operator="=", right_operand="x"),
]
expressions = spark.createDataFrame(expressions_data)

# Display the DataFrames
variables.show()
expressions.show()


# COMMAND ----------

variables.createOrReplaceTempView('variables')
expressions.createOrReplaceTempView('expressions')

# COMMAND ----------

# MAGIC %sql
# MAGIC select e.*, l.value as l_val, r.value as r_val,
# MAGIC   case when e.operator = '>' and l.value > r.value then True
# MAGIC        when e.operator = '<' and l.value < r.value then True
# MAGIC        when e.operator = '=' and l.value = r.value then true
# MAGIC        else False
# MAGIC   end as value
# MAGIC from expressions e
# MAGIC join variables l on e.left_operand = l.name
# MAGIC join variables r on e.right_operand = r.name

# COMMAND ----------

expressions.alias('e')\
    .join(variables.alias('l'), col('e.left_operand')==col('l.name'), how='inner')\
    .join(variables.alias('r'), col('e.right_operand')==col('r.name'), how='inner')\
    .select('left_operand','operator','right_operand', col('l.value').alias('l_val'), col('r.value').alias('r_val'),
                when((col('operator')=='>')&(col('l.value')>col('r.value')),True)\
                .when((col('operator')=='<')&(col('l.value')<col('r.value')),True)\
                .when((col('operator')=='=')&(col('l.value')==col('r.value')),True)\
                .otherwise(False)\
                .alias('value')
                )\
    .show()

# COMMAND ----------

expressions.alias('e')\
    .join(variables.alias('l'), col('e.left_operand')==col('l.name'), how='inner')\
    .join(variables.alias('r'), col('e.right_operand')==col('r.name'), how='inner')\
    .selectExpr('left_operand','operator','right_operand', 'l.value as l_val', 'r.value as r_val',
                """case when operator = '>' and l.value > r.value then True
                        when operator = '<' and l.value < r.value then True
                        when operator = '=' and l.value = r.value then True
                        else False
                    end as value
                """
                )\
    .show()


# COMMAND ----------

# print(variables.collect())
var = dict()
for row in variables.collect():
    var[row['name']] = row['value']

@udf(BooleanType())
def get_val(left_operand, operator, right_operand):
    left_val = var[left_operand]
    right_val = var[right_operand]
    if (operator == '>') & (left_val > right_val): return True
    elif (operator == '<') & (left_val < right_val): return True
    elif (operator == '=') & (left_val == right_val): return True
    else: return False


expressions.withColumn('value', get_val(col('left_operand'),col('operator'), col('right_operand'))).show()


# COMMAND ----------

# MAGIC %md
# MAGIC ###1212. Team Scores in Football Tournament

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("TeamsMatches").getOrCreate()

# Data for Teams table
teams_data = [
    (10, "Leetcode FC"),
    (20, "NewYork FC"),
    (30, "Atlanta FC"),
    (40, "Chicago FC"),
    (50, "Toronto FC")
]

# Create Teams DataFrame
teams = spark.createDataFrame(teams_data, ["team_id", "team_name"])

# Data for Matches table
matches_data = [
    (1, 10, 20, 3, 0),
    (2, 30, 10, 2, 2),
    (3, 10, 50, 5, 1),
    (4, 20, 30, 1, 0),
    (5, 50, 30, 1, 0)
]

# Create Matches DataFrame
matches = spark.createDataFrame(matches_data, ["match_id", "host_team", "guest_team", "host_goals", "guest_goals"])

# Show the DataFrames
print("Teams Table:")
teams.show()

print("Matches Table:")
matches.show()


# COMMAND ----------

teams.join(matches, (teams.team_id == matches.host_team) | ((teams.team_id == matches.guest_team) & (matches.guest_goals >= matches.host_goals)), how='left')\
    .groupBy('team_id','team_name').agg(
        sum(
            when(col('host_goals')>col('guest_goals'),3)\
            .when(col('host_goals')==col('guest_goals'),1)\
            .otherwise(0)
            ).alias('total_points')
    ).orderBy(col('total_points').desc(), 'team_id')\
    .show()

# COMMAND ----------

teams.createOrReplaceTempView('teams')
matches.createOrReplaceTempView('matches')

# COMMAND ----------

# MAGIC %sql
# MAGIC select team_id, team_name, sum(
# MAGIC   case when host_goals> guest_goals then 3
# MAGIC        when host_goals = guest_goals then 1
# MAGIC        else 0
# MAGIC   end
# MAGIC ) as num_points
# MAGIC from teams t
# MAGIC left join matches m on
# MAGIC   m.host_team = t.team_id or (
# MAGIC     m.guest_team = t.team_id 
# MAGIC     and
# MAGIC     m.guest_goals >= m.host_goals
# MAGIC   )
# MAGIC   group by all
# MAGIC   order by num_points desc, team_id

# COMMAND ----------

from pyspark.sql.functions import col, when, lit

# Calculate points as host team
host_points = matches.withColumn(
    "host_points",
    when(col("host_goals") > col("guest_goals"), lit(3))  # Win
    .when(col("host_goals") == col("guest_goals"), lit(1))  # Draw
    .otherwise(lit(0))  # Loss
).select(col("host_team").alias("team_id"), col("host_points"))

# Calculate points as guest team
guest_points = matches.withColumn(
    "guest_points",
    when(col("guest_goals") > col("host_goals"), lit(3))  # Win
    .when(col("guest_goals") == col("host_goals"), lit(1))  # Draw
    .otherwise(lit(0))  # Loss
).select(col("guest_team").alias("team_id"), col("guest_points"))

# Combine host and guest points
combined_points = host_points.union(guest_points)\
    .groupBy("team_id")\
    .agg(sum("host_points").alias('points'))\

# combined_points.show()

teams.alias('t').join(combined_points.alias('p'), teams.team_id == col('p.team_id'), how='left')\
    .select('t.team_id','team_name','points').fillna(0)\
    .orderBy(col('points').desc(), 'team_id')\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###1890. The Latest Login in 2020

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("LoginsDataFrame").getOrCreate()

# Data for Logins table
logins_data = [
    (6, "2020-06-30 15:06:07"),
    (6, "2021-04-21 14:06:06"),
    (6, "2019-03-07 00:18:15"),
    (8, "2020-02-01 05:10:53"),
    (8, "2020-12-30 00:46:50"),
    (2, "2020-01-16 02:49:50"),
    (2, "2019-08-25 07:59:08"),
    (14, "2019-07-14 09:00:00"),
    (14, "2021-01-06 11:59:59"),
]

# Create Logins DataFrame
logins = spark.createDataFrame(logins_data, ["user_id", "time_stamp"])

# Show the DataFrame
logins.show()


# COMMAND ----------

logins.filter(year(col('time_stamp'))==2020)\
    .groupBy('user_id').agg(max(col('time_stamp')).alias('last_stamp'))\
    .show()

# COMMAND ----------

logins.createOrReplaceTempView('logins')

# COMMAND ----------

# MAGIC %sql
# MAGIC select user_id, max(time_stamp) as last_stamp
# MAGIC from logins
# MAGIC where year(time_stamp) = 2020
# MAGIC group by all

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

activity.groupBy('player_id').agg(min(col('event_date')).alias('first_login')).show()

# COMMAND ----------

activity.createOrReplaceTempView('activity')

# COMMAND ----------

# MAGIC %sql
# MAGIC select player_id, min(event_date) as first_login
# MAGIC from activity
# MAGIC group by player_id

# COMMAND ----------

# MAGIC %md
# MAGIC ###1571. Warehouse Manager

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("DataFramesExample").getOrCreate()

# Data for Warehouse table
warehouse_data = [
    ("LCHouse1", 1, 1),
    ("LCHouse1", 2, 10),
    ("LCHouse1", 3, 5),
    ("LCHouse2", 1, 2),
    ("LCHouse2", 2, 2),
    ("LCHouse3", 4, 1)
]

# Schema for Warehouse table
warehouse_schema = StructType([
    StructField("name", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("units", IntegerType(), True)
])

# Creating Warehouse DataFrame
warehouse = spark.createDataFrame(data=warehouse_data, schema=warehouse_schema)

# Data for Products table
products_data = [
    (1, "LC-TV", 5, 50, 40),
    (2, "LC-KeyChain", 5, 5, 5),
    (3, "LC-Phone", 2, 10, 10),
    (4, "LC-T-Shirt", 4, 10, 20)
]

# Schema for Products table
products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("Width", IntegerType(), True),
    StructField("Length", IntegerType(), True),
    StructField("Height", IntegerType(), True)
])

# Creating Products DataFrame
products = spark.createDataFrame(data=products_data, schema=products_schema)

# Display DataFrames
warehouse.show()
products.show()


# COMMAND ----------

warehouse.alias('w').join(products.alias('p'), warehouse.product_id == products.product_id, how='inner')\
    .withColumn('volume', col('Width')*col('Length')*col('Height'))\
    .groupBy('w.name').agg(sum(col('units')*col('volume')).alias('volume'))\
    .orderBy('name')\
    .show()

# COMMAND ----------

warehouse.createOrReplaceTempView('warehouse')
products.createOrReplaceTempView('products')

# COMMAND ----------

# MAGIC %sql
# MAGIC select name, sum(units*width*length*height) as volume
# MAGIC from warehouse w
# MAGIC join products p on w.product_id = p.product_id
# MAGIC group by name
# MAGIC order by name

# COMMAND ----------

# MAGIC %md
# MAGIC ###586. Customer Placing the Largest Number of Orders

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("OrdersDataFrame").getOrCreate()

# Data for Orders table
orders_data = [
    (1, 1),
    (2, 2),
    (3, 3),
    (4, 3)
]

# Schema for Orders table
orders_schema = StructType([
    StructField("order_number", IntegerType(), True),
    StructField("customer_number", IntegerType(), True)
])

# Creating Orders DataFrame
orders = spark.createDataFrame(data=orders_data, schema=orders_schema)

# Display the DataFrame
orders.show()


# COMMAND ----------

orders.groupBy('customer_number').count()\
    .orderBy(col('count').desc())\
    .limit(1)\
    .select('customer_number')\
    .show()

# COMMAND ----------

orders.createOrReplaceTempView('orders')

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_number
# MAGIC from orders
# MAGIC group by 1
# MAGIC order by count(*) desc
# MAGIC limit 1

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

employees.groupBy('event_day','emp_id').agg(sum(col('out_time')-col('in_time')).alias('total_time')).show()

# COMMAND ----------

employees.createOrReplaceTempView('employees')

# COMMAND ----------

# MAGIC %sql
# MAGIC select event_day, emp_id, sum(out_time-in_time) as total_time
# MAGIC from employees
# MAGIC group by all

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



immediate = delivery.filter(col('order_date')==col('customer_pref_delivery_date')).count()
total_count = delivery.count()

immediate_percentage = builtins.round(immediate/total_count,2)

data = [
    (immediate_percentage,)
]

columns = ['immediate_percentage']

spark.createDataFrame(data, columns).show()

# COMMAND ----------

delivery.selectExpr(
    """
    round(
        sum(
            case when order_date = customer_pref_delivery_date then 1 else 0 end
        )/count(*),2
    ) as immediate_percentage
    """
).show()

# COMMAND ----------

delivery.select(
    round(
        sum(
            when(col("order_date") == col("customer_pref_delivery_date"), 1).otherwise(0)
        ) / count("*"),
        2
    ).alias("immediate_percentage")
).show()


# COMMAND ----------

delivery.createOrReplaceTempView('delivery')

# COMMAND ----------

# MAGIC %sql
# MAGIC select round(sum(case when order_date = customer_pref_delivery_date then 1 else 0 end)/count(*),2) as immediate_percentage
# MAGIC from delivery

# COMMAND ----------

# MAGIC %md
# MAGIC ###1445. Apples & Oranges

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("SalesDataFrame").getOrCreate()

# Data for Sales table
sales_data = [
    ("2020-05-01", "apples", 10),
    ("2020-05-01", "oranges", 8),
    ("2020-05-02", "apples", 15),
    ("2020-05-02", "oranges", 15),
    ("2020-05-03", "apples", 20),
    ("2020-05-03", "oranges", 0),
    ("2020-05-04", "apples", 15),
    ("2020-05-04", "oranges", 16),
]

# Schema for Sales table
sales_schema = StructType([
    StructField("sale_date", StringType(), True),
    StructField("fruit", StringType(), True),
    StructField("sold_num", IntegerType(), True)
])

# Creating Sales DataFrame
sales = spark.createDataFrame(data=sales_data, schema=sales_schema)

# Display the DataFrame
sales.show()


# COMMAND ----------

sales.groupBy('sale_date').agg(
    sum(when(col('fruit')=='apples',col('sold_num')).otherwise(lit(0))).alias('apples'),
    sum(when(col('fruit')=='oranges',col('sold_num')).otherwise(lit(0))).alias('oranges')
).selectExpr('sale_date','apples - oranges as diff').show()


# COMMAND ----------

sales.groupBy('sale_date').agg(
    sum(when(col('fruit')=='apples',col('sold_num')).otherwise(lit(0))).alias('apples'),
    sum(when(col('fruit')=='oranges',col('sold_num')).otherwise(lit(0))).alias('oranges')
).select('sale_date',(col('apples') - col('oranges')).alias('diff')).show()

# COMMAND ----------

sales.groupBy('sale_date').pivot('fruit').max()\
    .selectExpr('sale_date','apples-oranges as diff')\
    .show()

# COMMAND ----------

sales.createOrReplaceTempView('sales')

# COMMAND ----------

# MAGIC %sql
# MAGIC select sale_date, sum(case when fruit = 'apples' then sold_num else 0 end) - sum(case when fruit = 'oranges' then sold_num else 0 end) as diff
# MAGIC from sales
# MAGIC group by sale_date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sale_date, apples-oranges as diff
# MAGIC FROM sales
# MAGIC PIVOT (
# MAGIC     MAX(sold_num) FOR fruit IN ('apples', 'oranges')
# MAGIC )
# MAGIC order by 1;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###1699. Number of Calls Between Two Persons

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("CallsDataFrame").getOrCreate()

# Data for Calls table
calls_data = [
    (1, 2, 59),
    (2, 1, 11),
    (1, 3, 20),
    (3, 4, 100),
    (3, 4, 200),
    (3, 4, 200),
    (4, 3, 499)
]

# Schema for Calls table
calls_schema = StructType([
    StructField("from_id", IntegerType(), True),
    StructField("to_id", IntegerType(), True),
    StructField("duration", IntegerType(), True)
])

# Creating Calls DataFrame
calls = spark.createDataFrame(data=calls_data, schema=calls_schema)

# Display the DataFrame
calls.show()


# COMMAND ----------

groups = calls.filter(col('from_id')<col('to_id'))\
    .select('from_id','to_id')\
    .dropDuplicates()

# groups.join(calls, ((groups.from_id==calls.from_id) & (groups.to_id==calls.to_id))|((groups.from_id==calls.to_id) & ((groups.to_id==calls.from_id))), how='inner').show()

groups.alias('g').join(calls.alias('c'), ((col('g.from_id')==col('c.from_id')) & (col('g.to_id')==col('c.to_id')))|((col('g.from_id')==col('c.to_id')) & ((col('g.to_id')==col('c.from_id')))), how='inner')\
    .groupBy('g.from_id','g.to_id')\
    .agg(
        count('*').alias('call_count'),
        sum('duration').alias('duration')
    )\
    .selectExpr('from_id as person1','to_id as person2','call_count','duration')\
    .show()


# COMMAND ----------

calls.createOrReplaceTempView('calls')

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.from_id person1, a.to_id person2, count(*) as calls, sum(duration) as duration
# MAGIC from(select distinct from_id, to_id
# MAGIC from calls
# MAGIC where from_id < to_id) as a
# MAGIC join calls b on (a.from_id = b.from_id and a.to_id = b.to_id) or (a.from_id = b.to_id and a.to_id = b.from_id)
# MAGIC group by all

# COMMAND ----------

# MAGIC %md
# MAGIC ###1587. Bank Account Summary II

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("UsersAndTransactions").getOrCreate()

# ==================== Users DataFrame ====================
users_data = [
    (900001, "Alice"),
    (900002, "Bob"),
    (900003, "Charlie")
]

users_schema = StructType([
    StructField("account", IntegerType(), True),
    StructField("name", StringType(), True)
])

users = spark.createDataFrame(data=users_data, schema=users_schema)
users.show()

# ==================== Transactions DataFrame ====================
transactions_data = [
    (1, 900001, 7000, "2020-08-01"),
    (2, 900001, 7000, "2020-09-01"),
    (3, 900001, -3000, "2020-09-02"),
    (4, 900002, 1000, "2020-09-12"),
    (5, 900003, 6000, "2020-08-07"),
    (6, 900003, 6000, "2020-09-07"),
    (7, 900003, -4000, "2020-09-11")
]

transactions_schema = StructType([
    StructField("trans_id", IntegerType(), True),
    StructField("account", IntegerType(), True),
    StructField("amount", IntegerType(), True),
    StructField("transacted_on", StringType(), True)
])

transactions = spark.createDataFrame(data=transactions_data, schema=transactions_schema)
transactions.show()


# COMMAND ----------

users.alias('u').join(transactions.alias('t'), col('u.account')==col('t.account'), how='left')\
    .groupBy('u.account','name')\
    .agg(sum('amount').alias('balance'))\
    .filter(col('balance')>10000)\
    .select('name','balance')\
    .show()

# COMMAND ----------

users.createOrReplaceTempView('users')
transactions.createOrReplaceTempView('transactions')

# COMMAND ----------

# MAGIC %sql
# MAGIC select name, sum(amount) as balance
# MAGIC from users u
# MAGIC join transactions t on u.account = t.account
# MAGIC group by u.account, name
# MAGIC having balance > 10000

# COMMAND ----------

# MAGIC %md
# MAGIC ###182. Duplicate Emails

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize SparkSession
spark = SparkSession.builder.appName("PersonDataFrame").getOrCreate()

# Data for Person table
person_data = [
    (1, "a@b.com"),
    (2, "c@d.com"),
    (3, "a@b.com")
]

# Schema for Person table
person_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("email", StringType(), True)
])

# Creating Person DataFrame
person = spark.createDataFrame(data=person_data, schema=person_schema)

# Display the DataFrame
person.show()

# COMMAND ----------

person.groupBy('email').count().filter(col('count')>1).select('email').show()

# COMMAND ----------

person.createOrReplaceTempView('person')

# COMMAND ----------

# MAGIC %sql
# MAGIC select email
# MAGIC from person
# MAGIC group by all
# MAGIC having count(*) > 1

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

actor_director.groupBy('actor_id','director_id').count().filter(col('count')>2)\
    .select('actor_id','director_id')\
    .show()

# COMMAND ----------

actor_director.createOrReplaceTempView('actor_director')

# COMMAND ----------

# MAGIC %sql
# MAGIC select actor_id, director_id
# MAGIC from actor_director
# MAGIC group by all
# MAGIC having count(*) > 2

# COMMAND ----------

# MAGIC %md
# MAGIC ###1511. Customer Order Frequency

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# Initialize SparkSession
spark = SparkSession.builder.appName("CustomersProductsOrders").getOrCreate()

# ===================== Customers DataFrame =====================
customers_data = [
    (1, "Winston", "USA"),
    (2, "Jonathan", "Peru"),
    (3, "Moustafa", "Egypt")
]

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("country", StringType(), True)
])

customers = spark.createDataFrame(data=customers_data, schema=customers_schema)
customers.show()

# ===================== Product DataFrame =====================
product_data = [
    (10, "LC Phone", 300),
    (20, "LC T-Shirt", 10),
    (30, "LC Book", 45),
    (40, "LC Keychain", 2)
]

product_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("description", StringType(), True),
    StructField("price", IntegerType(), True)
])

product = spark.createDataFrame(data=product_data, schema=product_schema)
product.show()

# ===================== Orders DataFrame =====================
orders_data = [
    (1, 1, 10, "2020-06-10", 1),
    (2, 1, 20, "2020-07-01", 1),
    (3, 1, 30, "2020-07-08", 2),
    (4, 2, 10, "2020-06-15", 2),
    (5, 2, 40, "2020-07-01", 10),
    (6, 3, 20, "2020-06-24", 2),
    (7, 3, 30, "2020-06-25", 2),
    (9, 3, 30, "2020-05-08", 3)
]

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("order_date", StringType(), True),  # Can be changed to DateType for date-specific operations
    StructField("quantity", IntegerType(), True)
])

orders = spark.createDataFrame(data=orders_data, schema=orders_schema)
orders.show()


# COMMAND ----------

customers.alias('c').join(
    orders.filter((year(col('order_date'))==2020)&(month(col('order_date')).isin([6,7]))).alias('o'),
    col('c.customer_id')==col('o.customer_id'),
    how='inner'
).join(
    product.alias('p'),
    col('o.product_id')==col('p.product_id'),
    how = 'inner'
).withColumn('month', date_format('order_date','yyyyMM'))\
.groupBy('c.customer_id','name','month').agg(sum(col('quantity')*col('price')).alias('total_price'))\
.filter(col('total_price')>=100)\
.groupBy('customer_id','name').count().filter(col('count')>1)\
.select('customer_id','name')\
.show()

# COMMAND ----------

customers.createOrReplaceTempView('customers')
product.createOrReplaceTempView('product')
orders.createOrReplaceTempView('orders')

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id, name from(
# MAGIC select c.customer_id, name, date_format(order_date, 'yyyyMM') as ym, sum(quantity*price) as tot
# MAGIC from customers c
# MAGIC join orders o on c.customer_id = o.customer_id
# MAGIC join product p on p.product_id = o.product_id
# MAGIC where date_format(order_date, 'yyyyMM') in (202006, 202007)
# MAGIC group by all
# MAGIC having tot > 99)
# MAGIC group by all having count(*) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC ###1693. Daily Leads and Partners

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("DailySalesDataFrame").getOrCreate()

# Data for DailySales table
daily_sales_data = [
    ("2020-12-8", "toyota", 0, 1),
    ("2020-12-8", "toyota", 1, 0),
    ("2020-12-8", "toyota", 1, 2),
    ("2020-12-7", "toyota", 0, 2),
    ("2020-12-7", "toyota", 0, 1),
    ("2020-12-8", "honda", 1, 2),
    ("2020-12-8", "honda", 2, 1),
    ("2020-12-7", "honda", 0, 1),
    ("2020-12-7", "honda", 1, 2),
    ("2020-12-7", "honda", 2, 1)
]

# Schema for DailySales table
daily_sales_schema = StructType([
    StructField("date_id", StringType(), True),
    StructField("make_name", StringType(), True),
    StructField("lead_id", IntegerType(), True),
    StructField("partner_id", IntegerType(), True)
])

# Creating DailySales DataFrame
daily_sales = spark.createDataFrame(data=daily_sales_data, schema=daily_sales_schema)

# Display the DataFrame
daily_sales.show()


# COMMAND ----------

daily_sales.groupBy('date_id','make_name').agg(
    countDistinct(col('lead_id')).alias('unique_leads'),
    countDistinct(col('partner_id')).alias('unique_partners')
).show()

# COMMAND ----------

daily_sales.createOrReplaceTempView('daily_sales')

# COMMAND ----------

# MAGIC %sql
# MAGIC select date_id, make_name, count(distinct lead_id) as unique_leads, count(distinct partner_id) as unique_partners
# MAGIC from daily_sales
# MAGIC group by all

# COMMAND ----------

# MAGIC %md
# MAGIC ###1495. Friendly Movies Streamed Last Month

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("CreateDataFrames").getOrCreate()

# Data for TVProgram table
tv_program_data = [
    ("2020-06-10 08:00", 1, "LC-Channel"),
    ("2020-05-11 12:00", 2, "LC-Channel"),
    ("2020-05-12 12:00", 3, "LC-Channel"),
    ("2020-05-13 14:00", 4, "Disney Ch"),
    ("2020-06-18 14:00", 4, "Disney Ch"),
    ("2020-07-15 16:00", 5, "Disney Ch")
]

# Schema for TVProgram table
tv_program_columns = ["program_date", "content_id", "channel"]

# Creating the TVProgram DataFrame
tv_program = spark.createDataFrame(tv_program_data, tv_program_columns)

# Data for Content table
content_data = [
    (1, "Leetcode Movie", "N", "Movies"),
    (2, "Alg. for Kids", "Y", "Series"),
    (3, "Database Sols", "N", "Series"),
    (4, "Aladdin", "Y", "Movies"),
    (5, "Cinderella", "Y", "Movies")
]

# Schema for Content table
content_columns = ["content_id", "title", "Kids_content", "content_type"]

# Creating the Content DataFrame
content = spark.createDataFrame(content_data, content_columns)

# Show the DataFrames
tv_program.show()
content.show()


# COMMAND ----------

content.filter((col('Kids_content')=='Y') & (col('content_type')=='Movies'))\
    .join(tv_program.alias('p'), col('p.content_id')==content.content_id, how='inner')\
    .filter(date_format(col('program_date'),'yyyyMM')==202006)\
    .select('title')\
    .show()

# COMMAND ----------

tv_program.createOrReplaceTempView('tv_program')
content.createOrReplaceTempView('content')

# COMMAND ----------

# MAGIC %sql
# MAGIC select title
# MAGIC from content c
# MAGIC join tv_program p on c.content_id = p.content_id
# MAGIC where date_format(program_date, 'yyyyMM') = 202006
# MAGIC and kids_content = 'Y' and content_type = 'Movies'

# COMMAND ----------

# MAGIC %md
# MAGIC ###1501. Countries You Can Safely Invest In

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("CreateDataFrames").getOrCreate()

# Data for Person table
person_data = [
    (3, "Jonathan", "051-1234567"),
    (12, "Elvis", "051-7654321"),
    (1, "Moncef", "212-1234567"),
    (2, "Maroua", "212-6523651"),
    (7, "Meir", "972-1234567"),
    (9, "Rachel", "972-0011100")
]
person_columns = ["id", "name", "phone_number"]

# Creating Person DataFrame
person = spark.createDataFrame(person_data, person_columns)

# Data for Country table
country_data = [
    ("Peru", "051"),
    ("Israel", "972"),
    ("Morocco", "212"),
    ("Germany", "049"),
    ("Ethiopia", "251")
]
country_columns = ["name", "country_code"]

# Creating Country DataFrame
country = spark.createDataFrame(country_data, country_columns)

# Data for Calls table
calls_data = [
    (1, 9, 33),
    (2, 9, 4),
    (1, 2, 59),
    (3, 12, 102),
    (3, 12, 330),
    (12, 3, 5),
    (7, 9, 13),
    (7, 1, 3),
    (9, 7, 1),
    (1, 7, 7)
]
calls_columns = ["caller_id", "callee_id", "duration"]

# Creating Calls DataFrame
calls = spark.createDataFrame(calls_data, calls_columns)

# Show the DataFrames
person.show()
country.show()
calls.show()


# COMMAND ----------

calls.union(calls.select('callee_id','caller_id','duration')).alias('c')\
    .join(person.alias('p'), col('c.caller_id')==col('p.id'))\
    .join(country.alias('ct'), substring(col('phone_number'),1,3)==col('country_code'))\
    .groupBy('ct.name').agg(avg(col('duration')).alias('avg_duration'))\
    .withColumn('global_avg',lit(calls.agg(avg('duration')).collect()[0][0]))\
    .filter(col('avg_duration')>col('global_avg'))\
    .select('name')\
    .show()



# COMMAND ----------

person.createOrReplaceTempView('person')
country.createOrReplaceTempView('country')
calls.createOrReplaceTempView('calls')

# COMMAND ----------

# MAGIC %sql
# MAGIC select ct.name
# MAGIC from(
# MAGIC select *
# MAGIC from calls
# MAGIC union(
# MAGIC   select caller_id, caller_id, duration from calls
# MAGIC )) c
# MAGIC join person p on p.id = c.caller_id
# MAGIC join country ct on ct.country_code = left(phone_number,3)
# MAGIC group by ct.name
# MAGIC having avg(duration) > (select avg(duration) from calls)

# COMMAND ----------

# MAGIC %md
# MAGIC ###603. Consecutive Available Seats

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("CinemaSeats").getOrCreate()

# Data for the DataFrame
data = [
    (1, 1),
    (2, 0),
    (3, 1),
    (4, 1),
    (5, 1)
]

# Define the schema (column names)
columns = ["seat_id", "free"]

# Create the DataFrame
cinema = spark.createDataFrame(data, schema=columns)

# Show the DataFrame
cinema.show()


# COMMAND ----------

window_spec = Window.orderBy('seat_id')
cinema.withColumn('next_free',ifnull(lead(col('free')).over(window_spec), col('free')))\
    .filter(col('free')==col('next_free'))\
    .select('seat_id')\
    .show()

# COMMAND ----------

cinema.createOrReplaceTempView('cinema')

# COMMAND ----------

# MAGIC %sql
# MAGIC select seat_id
# MAGIC from cinema
# MAGIC qualify free = ifnull(lead(free) over(order by seat_id),free)

# COMMAND ----------

# MAGIC %md
# MAGIC ###1795. Rearrange Products Table

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("ProductsTable").getOrCreate()

# Data for the DataFrame
data = [
    (0, 95, 100, 105),
    (1, 70, None, 80)  # 'null' in Python is represented as 'None'
]

# Define the schema (column names)
columns = ["product_id", "store1", "store2", "store3"]

# Create the DataFrame
products = spark.createDataFrame(data, schema=columns)

# Show the DataFrame
products.show()


# COMMAND ----------

products.melt(ids='product_id', values=['store1','store2','store3'], variableColumnName='store', valueColumnName='price').dropna().show()

# COMMAND ----------

products.createOrReplaceTempView('products')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from products
# MAGIC unpivot
# MAGIC (
# MAGIC   price for store in (store1, store2, store3)
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ###613. Shortest Distance in a Line

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("PointDataFrame").getOrCreate()

# Data for the DataFrame
data = [
    (-1,),
    (0,),
    (2,)
]

# Define the schema (column name)
columns = ["x"]

# Create the DataFrame
points = spark.createDataFrame(data, schema=columns)

# Show the DataFrame
points.show()


# COMMAND ----------

points.alias('a').crossJoin(points.alias('b'))\
    .filter((col('a.x')!=col('b.x'))&((col('a.x')-col('b.x'))>0))\
    .select(min(abs(col('a.x')-col('b.x'))).alias('shortest'))\
    .show()

# COMMAND ----------

points.createOrReplaceTempView('points')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select min(a.x-b.x) as shortest
# MAGIC select min(abs(a.x-b.x))
# MAGIC from points a
# MAGIC cross join points b
# MAGIC where a.x-b.x > 0 and a.x != b.x

# COMMAND ----------

# MAGIC %md
# MAGIC ###1965. Employees With Missing Information

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("EmployeeSalaries").getOrCreate()

# Data for the Employees table
employees_data = [
    (2, "Crew"),
    (4, "Haven"),
    (5, "Kristian")
]

# Data for the Salaries table
salaries_data = [
    (5, 76071),
    (1, 22517),
    (4, 63539)
]

# Define the schemas (column names)
employees_columns = ["employee_id", "name"]
salaries_columns = ["employee_id", "salary"]

# Create DataFrames
employees = spark.createDataFrame(employees_data, schema=employees_columns)
salaries = spark.createDataFrame(salaries_data, schema=salaries_columns)

# Show the DataFrames
employees.show()
salaries.show()


# COMMAND ----------

employees.alias('e').join(salaries.alias('s'), employees.employee_id==salaries.employee_id, how='full')\
    .filter(col('name').isNull() | col('salary').isNull())\
    .select(ifnull(col('e.employee_id'),col('s.employee_id')).alias('employee_id'))\
    .show()

# COMMAND ----------

employees.createOrReplaceTempView('employees')
salaries.createOrReplaceTempView('salaries')

# COMMAND ----------

# MAGIC %sql
# MAGIC select ifnull(e.employee_id, s.employee_id) as employee_id
# MAGIC from employees e
# MAGIC full join salaries s on e.employee_id = s.employee_id
# MAGIC where e.name is null or s.salary is null

# COMMAND ----------

# MAGIC %md
# MAGIC ###1264. Page Recommendations

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("CreateDataFrames").getOrCreate()

# Define schema and data for Friendship table
friendship_schema = StructType([
    StructField("user1_id", IntegerType(), True),
    StructField("user2_id", IntegerType(), True)
])

friendship_data = [
    (1, 2), (1, 3), (1, 4),
    (2, 3), (2, 4), (2, 5),
    (6, 1)
]

friendship = spark.createDataFrame(data=friendship_data, schema=friendship_schema)

# Define schema and data for Likes table
likes_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("page_id", IntegerType(), True)
])

likes_data = [
    (1, 88), (2, 23), (3, 24),
    (4, 56), (5, 11), (6, 33),
    (2, 77), (3, 77), (6, 88)
]

likes = spark.createDataFrame(data=likes_data, schema=likes_schema)

# Show the DataFrames
friendship.show()
likes.show()


# COMMAND ----------

friends = friendship.filter(col('user1_id')==1).select('user2_id').union(
    friendship.filter(col('user2_id')==1).select('user1_id')
).withColumnRenamed('user2_id','user_id')

likes.alias('a').join(likes.filter(col('user_id')==1).select('page_id').alias('b'),
                      col('a.page_id')==col('b.page_id'), 
                      how='left_anti'                  
).join(friends, friends.user_id==col('a.user_id'))\
    .selectExpr('page_id as recommended_pages')\
    .dropDuplicates()\
    .show()

# COMMAND ----------

friendship.createOrReplaceTempView('friendship')
likes.createOrReplaceTempView('likes')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct page_id as recommended_pages
# MAGIC from(
# MAGIC select * from likes
# MAGIC where page_id not in (
# MAGIC select page_id
# MAGIC from likes
# MAGIC where user_id = 1)) a
# MAGIC join (
# MAGIC select user2_id as user_id from friendship where user1_id = 1
# MAGIC union
# MAGIC select user1_id as user_id from friendship where user2_id = 1
# MAGIC ) b on a.user_id = b.user_id

# COMMAND ----------

# MAGIC %md
# MAGIC ###608. Tree Node

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("TreeDataFrame").getOrCreate()

# Define schema and data for Tree table
tree_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("p_id", IntegerType(), True)
])

tree_data = [
    (1, None),  # Representing NULL as None in Python
    (2, 1),
    (3, 1),
    (4, 2),
    (5, 2)
]

tree = spark.createDataFrame(data=tree_data, schema=tree_schema)

# Show the DataFrame
tree.show()


# COMMAND ----------

tree.alias('a').join(tree.select('p_id').dropDuplicates().alias('b'), col('a.id')==col('b.p_id'),how='left')\
    .withColumn('type',when(col('a.p_id').isNull(),'root')\
            .when(col('b.p_id').isNull(),'leaf')\
            .otherwise('inner')
                )\
    .select('a.id','type')\
    .dropDuplicates()\
    .show()

# COMMAND ----------

tree.createOrReplaceTempView('tree')

# COMMAND ----------

# MAGIC %sql
# MAGIC select id, case when a.p_id is null then 'root'
# MAGIC                 when b.p_id is null then 'leaf'
# MAGIC                 else 'inner'
# MAGIC             end as type
# MAGIC from tree a
# MAGIC left join (select distinct p_id from tree) b on a.id = b.p_id

# COMMAND ----------

# MAGIC %md
# MAGIC ###534. Game Play Analysis III

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize a Spark session
spark = SparkSession.builder.appName("ActivityTable").getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("player_id", IntegerType(), True),
    StructField("device_id", IntegerType(), True),
    StructField("event_date", StringType(), True),
    StructField("games_played", IntegerType(), True)
])

# Create the data as a list of tuples
data = [
    (1, 2, "2016-03-01", 5),
    (1, 2, "2016-05-02", 6),
    (1, 3, "2017-06-25", 1),
    (3, 1, "2016-03-02", 0),
    (3, 4, "2018-07-03", 5)
]

# Create the DataFrame named 'activity'
activity = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
activity.show()


# COMMAND ----------

window_spec = Window.partitionBy('player_id').orderBy(col('player_id').asc(),col('event_date').asc())

activity.withColumn('games_played_so_far',sum('games_played').over(window_spec))\
    .select('player_id','event_date','games_played_so_far')\
    .show()

# COMMAND ----------



# COMMAND ----------

activity.createOrReplaceTempView('activity')

# COMMAND ----------

# MAGIC %sql
# MAGIC select player_id, 
# MAGIC   event_date,
# MAGIC   sum(games_played) over(partition by player_id order by player_id, event_date) as games_played_so_far
# MAGIC from activity

# COMMAND ----------

# MAGIC %md
# MAGIC ###1783. Grand Slam Titles

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark Session
spark = SparkSession.builder.appName("TablesCreation").getOrCreate()

# Define the schema for Players table
players_schema = StructType([
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True)
])

# Create data for Players table
players_data = [
    (1, "Nadal"),
    (2, "Federer"),
    (3, "Novak")
]

# Create Players DataFrame
players = spark.createDataFrame(players_data, schema=players_schema)

# Define the schema for Championships table
championships_schema = StructType([
    StructField("year", IntegerType(), True),
    StructField("Wimbledon", IntegerType(), True),
    StructField("Fr_open", IntegerType(), True),
    StructField("US_open", IntegerType(), True),
    StructField("Au_open", IntegerType(), True)
])

# Create data for Championships table
championships_data = [
    (2018, 1, 1, 1, 1),
    (2019, 1, 1, 2, 2),
    (2020, 2, 1, 2, 2)
]

# Create Championships DataFrame
championships = spark.createDataFrame(championships_data, schema=championships_schema)

# Show Players DataFrame
players.show()

# Show Championships DataFrame
championships.show()



# COMMAND ----------

players.alias('p').join(
    championships.melt(ids='year',values=['Wimbledon','Fr_open','US_open','Au_open'], variableColumnName='name', valueColumnName='player_id').alias('c'),
    col('p.player_id')==col('c.player_id'),
    how='inner'
    ).groupBy('p.player_id','player_name').agg(sum(lit(1)).alias('grand_slams_count')).show()

# COMMAND ----------

players.createOrReplaceTempView('players')
championships.createOrReplaceTempView('championships')

# COMMAND ----------

# MAGIC %sql
# MAGIC select p.player_id, player_name, sum(1) as grand_slams_count
# MAGIC from players p
# MAGIC join(
# MAGIC select *
# MAGIC from championships
# MAGIC unpivot(
# MAGIC   player_id for name in (Wimbledon,Fr_open,US_open,Au_open)
# MAGIC )) as c on p.player_id = c.player_id
# MAGIC group by all

# COMMAND ----------

# MAGIC %md
# MAGIC ###1747. Leetflex Banned Accounts

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType
from datetime import datetime

# Initialize a Spark session
spark = SparkSession.builder.appName("LogInfoTable").getOrCreate()

# Define the schema for LogInfo table
loginfo_schema = StructType([
    StructField("account_id", IntegerType(), True),
    StructField("ip_address", IntegerType(), True),
    StructField("login", TimestampType(), True),
    StructField("logout", TimestampType(), True)
])

# Create the data for LogInfo table with datetime objects
loginfo_data = [
    (1, 1, datetime(2021, 2, 1, 9, 0, 0), datetime(2021, 2, 1, 9, 30, 0)),
    (1, 2, datetime(2021, 2, 1, 8, 0, 0), datetime(2021, 2, 1, 11, 30, 0)),
    (2, 6, datetime(2021, 2, 1, 20, 30, 0), datetime(2021, 2, 1, 22, 0, 0)),
    (2, 7, datetime(2021, 2, 2, 20, 30, 0), datetime(2021, 2, 2, 22, 0, 0)),
    (3, 9, datetime(2021, 2, 1, 16, 0, 0), datetime(2021, 2, 1, 16, 59, 59)),
    (3, 13, datetime(2021, 2, 1, 17, 0, 0), datetime(2021, 2, 1, 17, 59, 59)),
    (4, 10, datetime(2021, 2, 1, 16, 0, 0), datetime(2021, 2, 1, 17, 0, 0)),
    (4, 11, datetime(2021, 2, 1, 17, 0, 0), datetime(2021, 2, 1, 17, 59, 59))
]

# Create the DataFrame
loginfo = spark.createDataFrame(loginfo_data, schema=loginfo_schema)

# Show the DataFrame
loginfo.show()


# COMMAND ----------

loginfo.alias('a').join(loginfo.alias('b'), 
                        (col('a.account_id')==col('b.account_id')) & (col('a.ip_address')!=col('b.ip_address')),
                        how='inner'
                        )\
    .filter(col('b.login').between(col('a.login'),col('a.logout')))\
    .select('a.account_id')\
    .display()

# COMMAND ----------

loginfo.createOrReplaceTempView('loginfo')

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.account_id
# MAGIC from loginfo a
# MAGIC join loginfo b on a.account_id = b.account_id and a.ip_address <> b.ip_address
# MAGIC where b.login between a.login and a.logout

# COMMAND ----------

# MAGIC %md
# MAGIC ###1350. Students With Invalid Departments

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark Session
spark = SparkSession.builder.appName("CreateTables").getOrCreate()

# Define the schema for Departments table
departments_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

# Create data for Departments table
departments_data = [
    (1, "Electrical Engineering"),
    (7, "Computer Engineering"),
    (13, "Bussiness Administration")
]

# Create Departments DataFrame
departments = spark.createDataFrame(departments_data, schema=departments_schema)

# Define the schema for Students table
students_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department_id", IntegerType(), True)
])

# Create data for Students table
students_data = [
    (23, "Alice", 1),
    (1, "Bob", 7),
    (5, "Jennifer", 13),
    (2, "John", 14),
    (4, "Jasmine", 77),
    (3, "Steve", 74),
    (6, "Luis", 1),
    (8, "Jonathan", 7),
    (7, "Daiana", 33),
    (11, "Madelynn", 1)
]

# Create Students DataFrame
students = spark.createDataFrame(students_data, schema=students_schema)

# Show Departments DataFrame
departments.show()

# Show Students DataFrame
students.show()


# COMMAND ----------

students.join(departments, students.department_id==departments.id, how='left_anti')\
    .select(students.id, students.name)\
    .show()

# COMMAND ----------

students.createOrReplaceTempView('students')
departments.createOrReplaceTempView('departments')

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.id, a.name
# MAGIC from students a
# MAGIC left anti join departments b on a.department_id = b.id

# COMMAND ----------

# MAGIC %md
# MAGIC ###1303. Find the Team Size

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

# Initialize Spark Session
spark = SparkSession.builder.appName("EmployeeTable").getOrCreate()

# Define the schema for the Employee table
employee_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("team_id", IntegerType(), True)
])

# Create the data for the Employee table
employee_data = [
    (1, 8),
    (2, 8),
    (3, 8),
    (4, 7),
    (5, 9),
    (6, 9)
]

# Create the Employee DataFrame
employee = spark.createDataFrame(employee_data, schema=employee_schema)

# Show the Employee DataFrame
employee.show()


# COMMAND ----------

employee.alias('e').join(employee.groupBy('team_id').count().alias('b'),
                         col('e.team_id')==col('b.team_id'), how='left'
                         )\
                         .selectExpr('employee_id','count as team_size')\
                         .show()

# COMMAND ----------

employee.createOrReplaceTempView('employee')

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.employee_id, team_size
# MAGIC from employee a
# MAGIC left join(
# MAGIC select team_id, count(*) as team_size
# MAGIC from employee
# MAGIC group by team_id) b on a.team_id = b.team_id

# COMMAND ----------

# MAGIC %md
# MAGIC ###512. Game Play Analysis II

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark Session
spark = SparkSession.builder.appName("ActivityTable").getOrCreate()

# Define the schema for Activity table
activity_schema = StructType([
    StructField("player_id", IntegerType(), True),
    StructField("device_id", IntegerType(), True),
    StructField("event_date", StringType(), True),
    StructField("games_played", IntegerType(), True)
])

# Create the data for Activity table
activity_data = [
    (1, 2, "2016-03-01", 5),
    (1, 2, "2016-05-02", 6),
    (2, 3, "2017-06-25", 1),
    (3, 1, "2016-03-02", 0),
    (3, 4, "2018-07-03", 5)
]

# Create the DataFrame
activity = spark.createDataFrame(activity_data, schema=activity_schema)

# Show the DataFrame
activity.show()


# COMMAND ----------

activity.alias('a').join(activity.groupBy('player_id').agg(min('event_date').alias('event_date')).alias('b'),
                         (col('a.player_id')==col('b.player_id')) & (col('a.event_date')==col('b.event_date')),
                         how='inner'
                         )\
                         .selectExpr('a.player_id','device_id')\
                         .show()

# COMMAND ----------

activity.createOrReplaceTempView('activity')

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.player_id, device_id
# MAGIC from activity a
# MAGIC join(
# MAGIC select player_id, min(event_date) as event_date
# MAGIC from activity group by all) b on a.player_id = b.player_id and a.event_date = b.event_date

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

department.alias('d').join(employee.alias('e'), col('d.id')==col('e.departmentId'), how='inner')\
    .withColumn('rank',rank().over(Window.partitionBy('d.id').orderBy(col('salary').desc())))\
    .filter(col('rank')==1)\
    .selectExpr('d.name as Department','e.name as Employee','Salary')\
    .show()

# COMMAND ----------

employee.createOrReplaceTempView('employee')
department.createOrReplaceTempView('department')

# COMMAND ----------

# MAGIC %sql
# MAGIC select d.name Department, e.name Employee, Salary
# MAGIC from department d
# MAGIC join employee e on e.departmentId = d.id
# MAGIC qualify rank() over (partition by d.id order by salary desc) = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ###1549. The Most Recent Orders for Each Product

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Create DataFrames Example") \
    .getOrCreate()

# Step 1: Prepare Data
customers_data = [
    (1, 'Winston'),
    (2, 'Jonathan'),
    (3, 'Annabelle'),
    (4, 'Marwan'),
    (5, 'Khaled')
]

orders_data = [
    (1, '2020-07-31', 1, 1),
    (2, '2020-07-30', 2, 2),
    (3, '2020-08-29', 3, 3),
    (4, '2020-07-29', 4, 1),
    (5, '2020-06-10', 1, 2),
    (6, '2020-08-01', 2, 1),
    (7, '2020-08-01', 3, 1),
    (8, '2020-08-03', 1, 2),
    (9, '2020-08-07', 2, 3),
    (10, '2020-07-15', 1, 2)
]

products_data = [
    (1, 'keyboard', 120),
    (2, 'mouse', 80),
    (3, 'screen', 600),
    (4, 'hard disk', 450)
]

# Step 2: Define Schemas
customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True)
])

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True)
])

products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("price", IntegerType(), True)
])

# Step 3: Create DataFrames
customers = spark.createDataFrame(customers_data, schema=customers_schema)
orders = spark.createDataFrame(orders_data, schema=orders_schema)
products = spark.createDataFrame(products_data, schema=products_schema)

# Step 4: Show DataFrames
print("Customers DataFrame:")
customers.show()

print("Orders DataFrame:")
orders.show()

print("Products DataFrame:")
products.show()


# COMMAND ----------

products.alias('p').join(
    orders.withColumn('rank',rank().over(Window.partitionBy('product_id').orderBy(col('order_date').desc()))).filter(col('rank')==1).alias('o'),
    col('p.product_id')==col('o.product_id'),
    how='inner'
    )\
    .select('p.product_name','p.product_id','order_id','order_date')\
    .orderBy('product_name','product_id','order_id')\
    .show()

# orders.withColumn('rank',rank().over(Window.partitionBy('product_id').orderBy(col('order_date').desc()))).show()

# COMMAND ----------

products.createOrReplaceTempView('products')
orders.createOrReplaceTempView('orders')

# COMMAND ----------

# MAGIC %sql
# MAGIC select p.product_name, p.product_id, order_id, order_date
# MAGIC from products p
# MAGIC join(
# MAGIC select order_id, product_id, order_date, rank() over (partition by product_id order by order_date desc) as rank
# MAGIC from orders) o on o.product_id = p.product_id
# MAGIC where rank = 1
# MAGIC order by 1,2,3

# COMMAND ----------

# MAGIC %md
# MAGIC ###1532. The Most Recent Three Orders

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Create DataFrames Example") \
    .getOrCreate()

# Step 1: Define Data
customers_data = [
    (1, 'Winston'),
    (2, 'Jonathan'),
    (3, 'Annabelle'),
    (4, 'Marwan'),
    (5, 'Khaled')
]

orders_data = [
    (1, '2020-07-31', 1, 30),
    (2, '2020-07-30', 2, 40),
    (3, '2020-07-31', 3, 70),
    (4, '2020-07-29', 4, 100),
    (5, '2020-06-10', 1, 1010),
    (6, '2020-08-01', 2, 102),
    (7, '2020-08-01', 3, 111),
    (8, '2020-08-03', 1, 99),
    (9, '2020-08-07', 2, 32),
    (10, '2020-07-15', 1, 2)
]

# Step 2: Define Schemas
customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True)
])

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("cost", IntegerType(), True)
])

# Step 3: Create DataFrames
customers = spark.createDataFrame(customers_data, schema=customers_schema)
orders = spark.createDataFrame(orders_data, schema=orders_schema)

# Step 4: Show DataFrames
print("Customers DataFrame:")
customers.show()

print("Orders DataFrame:")
orders.show()


# COMMAND ----------

customers.alias('c').join(
    orders.withColumn('rank',dense_rank().over(Window.partitionBy('customer_id').orderBy(col('order_date').desc()))).filter(col('rank')<=3).alias('o'),
    col('c.customer_id')==col('o.customer_id'),
    how='inner'
).select('name','c.customer_id','order_id','order_date')\
 .orderBy('name','customer_id',col('order_date').desc())\
 .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###1831. Maximum Transaction Each Day

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Transactions Table Example") \
    .getOrCreate()

# Define Data
transactions_data = [
    (8, '2021-4-3 15:57:28', 57),
    (9, '2021-4-28 08:47:25', 21),
    (1, '2021-4-29 13:28:30', 58),
    (5, '2021-4-28 16:39:59', 40),
    (6, '2021-4-29 23:39:28', 58)
]

# Define Schema
transactions_schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("day", StringType(), True),
    StructField("amount", IntegerType(), True)
])

# Create DataFrame
transactions = spark.createDataFrame(transactions_data, schema=transactions_schema)

# Show DataFrame
print("Transactions DataFrame:")
transactions.show()


# COMMAND ----------

transactions.select(date_format('day','yyyyMMdd')).show()

# COMMAND ----------

transactions.withColumn('rank',rank().over(Window.partitionBy(date_format('day','yyyyMMdd')).orderBy(col('amount').desc())))\
    .filter(col('rank')==1)\
    .select('transaction_id')\
    .orderBy('transaction_id')\
    .show()

# COMMAND ----------

transactions.createOrReplaceTempView('transactions')

# COMMAND ----------

# MAGIC %sql
# MAGIC select transaction_id
# MAGIC from transactions
# MAGIC qualify rank() over (partition by date_format(day, 'yyyyMMdd') order by amount desc) = 1
# MAGIC order by 1

# COMMAND ----------

# MAGIC %md
# MAGIC ###1077. Project Employees III

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Project and Employee DataFrames") \
    .getOrCreate()

# Define Data for Project Table
project_data = [
    (1, 1),
    (1, 2),
    (1, 3),
    (2, 1),
    (2, 4)
]

# Define Data for Employee Table
employee_data = [
    (1, 'Khaled', 3),
    (2, 'Ali', 2),
    (3, 'John', 3),
    (4, 'Doe', 2)
]

# Define Schemas
project_schema = StructType([
    StructField("project_id", IntegerType(), True),
    StructField("employee_id", IntegerType(), True)
])

employee_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("experience_years", IntegerType(), True)
])

# Create DataFrames
project = spark.createDataFrame(project_data, schema=project_schema)
employee = spark.createDataFrame(employee_data, schema=employee_schema)

# Show DataFrames
print("Project DataFrame:")
project.show()

print("Employee DataFrame:")
employee.show()


# COMMAND ----------

project.join(employee, project.employee_id==employee.employee_id, how='inner')\
    .withColumn('rank',rank().over(Window.partitionBy('project_id').orderBy(col('experience_years').desc())))\
    .filter(col('rank')==1)\
    .select('project_id',project.employee_id)\
    .show()

# COMMAND ----------

project.createOrReplaceTempView('project')
employee.createOrReplaceTempView('employee')

# COMMAND ----------

# MAGIC %sql
# MAGIC select project_id, p.employee_id
# MAGIC from project p
# MAGIC join employee e on e.employee_id = p.employee_id
# MAGIC qualify rank() over (partition by project_id order by experience_years desc) = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ###1285. Find the Start and End Number of Continuous Ranges

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Logs Table Example") \
    .getOrCreate()

# Define Data for Logs Table
logs_data = [
    (1,),
    (2,),
    (3,),
    (7,),
    (8,),
    (10,)
]

# Define Schema
logs_schema = StructType([
    StructField("log_id", IntegerType(), True)
])

# Create DataFrame
logs = spark.createDataFrame(logs_data, schema=logs_schema)

# Show DataFrame
print("Logs DataFrame:")
logs.show()


# COMMAND ----------

logs.withColumn('diff', logs.log_id-row_number().over(Window.orderBy('log_id')))\
    .groupBy('diff').agg(min('log_id').alias('start'),max('log_id').alias('end'))\
    .select('start','end')\
    .show()

# COMMAND ----------

logs.createOrReplaceTempView('logs')

# COMMAND ----------

# MAGIC %sql
# MAGIC select min(log_id) as start, max(log_id) as end
# MAGIC from(
# MAGIC select log_id, log_id - row_number() over (order by log_id) rn
# MAGIC from logs
# MAGIC )
# MAGIC group by rn
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###1596 - The Most Frequently Ordered Products for Each Customer

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Customers, Orders, Products Tables") \
    .getOrCreate()

# Define Data for Customers Table
customers_data = [
    (1, 'Alice'),
    (2, 'Bob'),
    (3, 'Tom'),
    (4, 'Jerry'),
    (5, 'John')
]

# Define Schema for Customers Table
customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True)
])

# Create Customers DataFrame
customers = spark.createDataFrame(customers_data, schema=customers_schema)

# Define Data for Orders Table
orders_data = [
    (1, '2020-07-31', 1, 1),
    (2, '2020-07-30', 2, 2),
    (3, '2020-08-29', 3, 3),
    (4, '2020-07-29', 4, 1),
    (5, '2020-06-10', 1, 2),
    (6, '2020-08-01', 2, 1),
    (7, '2020-08-01', 3, 3),
    (8, '2020-08-03', 1, 2),
    (9, '2020-08-07', 2, 3),
    (10, '2020-07-15', 1, 2)
]

# Define Schema for Orders Table
orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True)
])

# Create Orders DataFrame
orders = spark.createDataFrame(orders_data, schema=orders_schema)

# Define Data for Products Table
products_data = [
    (1, 'keyboard', 120),
    (2, 'mouse', 80),
    (3, 'screen', 600),
    (4, 'hard disk', 450)
]

# Define Schema for Products Table
products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("price", IntegerType(), True)
])

# Create Products DataFrame
products = spark.createDataFrame(products_data, schema=products_schema)

# Show DataFrames
print("Customers DataFrame:")
customers.show()

print("Orders DataFrame:")
orders.show()

print("Products DataFrame:")
products.show()


# COMMAND ----------

orders.groupBy('customer_id','product_id').count()\
    .withColumn('rank',rank().over(Window.partitionBy('customer_id').orderBy(col('count').desc())))\
    .filter(col('rank')==1)\
    .join(products, orders.product_id==products.product_id, how='inner')\
    .select('customer_id',orders.product_id, 'product_name')\
    .orderBy('customer_id','product_id')\
    .show()

# COMMAND ----------

orders.createOrReplaceTempView('orders')
products.createOrReplaceTempView('products')

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id, o.product_id, product_name
# MAGIC from(
# MAGIC select customer_id, product_id, count(*) as count
# MAGIC from orders o
# MAGIC group by all
# MAGIC ) o
# MAGIC join products p on p.product_id = o.product_id
# MAGIC qualify rank() over(partition by customer_id order by count desc) = 1

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Create user_visits DataFrame").getOrCreate()

# Data for the UserVisits table
data = [
    (1, "2020-11-28"),
    (1, "2020-10-20"),
    (1, "2020-12-03"),
    (2, "2020-10-05"),
    (2, "2020-12-09"),
    (3, "2020-11-11")
]

# Schema for the DataFrame
columns = ["user_id", "visit_date"]

# Create the DataFrame and name it user_visits
user_visits = spark.createDataFrame(data, columns)

# Show the DataFrame
user_visits.show()


# COMMAND ----------

user_visits.withColumn('window', date_diff(ifnull(lead('visit_date').over(Window.partitionBy('user_id').orderBy('visit_date')), lit('2021-01-01')), col('visit_date')))\
    .groupBy('user_id').agg(max('window').alias('biggest_window'))\
    .show()

# COMMAND ----------

user_visits.createOrReplaceTempView('user_visits')

# COMMAND ----------

# MAGIC %sql
# MAGIC select user_id, max(window) as biggest_window from(
# MAGIC select user_id, date_diff(ifnull(lead(visit_date) over(partition by user_id order by visit_date), '2021-01-01'), visit_date) as window
# MAGIC from user_visits)
# MAGIC group by all

# COMMAND ----------

# MAGIC %md
# MAGIC ###1270. All People Report to the Given Manager

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Create Employees DataFrame").getOrCreate()

# Data for the Employees table
data = [
    (1, "Boss", 1),
    (3, "Alice", 3),
    (2, "Bob", 1),
    (4, "Daniel", 2),
    (7, "Luis", 4),
    (8, "Jhon", 3),
    (9, "Angela", 8),
    (77, "Robert", 1)
]

# Schema for the DataFrame
columns = ["employee_id", "employee_name", "manager_id"]

# Create the DataFrame and name it employees
employees = spark.createDataFrame(data, columns)

# Show the DataFrame
employees.show()


# COMMAND ----------

employees.alias('a1').join(employees.alias('a2'), col('a1.manager_id')==col('a2.employee_id'), how='left')\
    .join(employees.alias('a3'), col('a2.manager_id')==col('a3.employee_id'), how='left')\
    .join(employees.alias('a4'), col('a3.manager_id')==col('a4.employee_id'), how='left')\
    .filter(((col('a2.employee_id')==1)|(col('a3.employee_id')==1)|(col('a4.employee_id')==1))&(col('a1.employee_id')!=1))\
    .select('a1.employee_id')\
    .show()

# COMMAND ----------

employees.createOrReplaceTempView('employees')

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.employee_id
# MAGIC from employees a
# MAGIC left join employees a1 on a.manager_id = a1.employee_id
# MAGIC left join employees a2 on a1.manager_id = a2.employee_id
# MAGIC left join employees a3 on a2.manager_id = a3.employee_id
# MAGIC where (a1.employee_id = 1 or a2.employee_id = 1 or a3.employee_id = 1) and a.employee_id <> 1

# COMMAND ----------

# MAGIC %md
# MAGIC ###1412. Find the Quiet Students in All Exams

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Create Tables DataFrames").getOrCreate()

# Data for the Student table
student_data = [
    (1, "Daniel"),
    (2, "Jade"),
    (3, "Stella"),
    (4, "Jonathan"),
    (5, "Will")
]

# Schema for the Student DataFrame
student_columns = ["student_id", "student_name"]

# Create the Student DataFrame
student = spark.createDataFrame(student_data, student_columns)

# Show the Student DataFrame
student.show()

# Data for the Exam table
exam_data = [
    (10, 1, 70),
    (10, 2, 80),
    (10, 3, 90),
    (20, 1, 80),
    (30, 1, 70),
    (30, 3, 80),
    (30, 4, 90),
    (40, 1, 60),
    (40, 2, 70),
    (40, 4, 80)
]

# Schema for the Exam DataFrame
exam_columns = ["exam_id", "student_id", "score"]

# Create the Exam DataFrame
exam = spark.createDataFrame(exam_data, exam_columns)

# Show the Exam DataFrame
exam.show()


# COMMAND ----------

# exam.select(max('score').alias('max'),min('score').alias('min')).show()

student.alias('s').join(exam.alias('e'), col('s.student_id')==col('e.student_id'), how='inner')\
    .crossJoin(exam.select(max('score').alias('max'),min('score').alias('min')))\
    .withColumn('quite', when((col('score')!=col('min'))&(col('score')!=col('max')),1).otherwise(0))\
    .groupBy(col('s.student_id'),'student_name').agg(count('*').alias('count'), sum(col('quite')).alias('quite'))\
    .filter(col('count')==col('quite'))\
    .select('student_id','student_name')\
    .show()

# COMMAND ----------

student.createOrReplaceTempView('student')
exam.createOrReplaceTempView('exam')

# COMMAND ----------

# MAGIC %sql
# MAGIC select student_id, student_name from(
# MAGIC select s.student_id, student_name, score, case when score <> min and score <> max then 1 else 0 end as quite
# MAGIC from student s
# MAGIC join exam e on s.student_id=e.student_id
# MAGIC cross join (select max(score) max, min(score) min from exam))
# MAGIC group by all
# MAGIC having count(*) = sum(quite)

# COMMAND ----------

# MAGIC %md
# MAGIC ###1767 - Find the Subtasks That Did Not Execute

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Create Tasks and Executed DataFrames").getOrCreate()

# Data for the Tasks table
tasks_data = [
    (1, 3),
    (2, 2),
    (3, 4)
]

# Schema for the Tasks DataFrame
tasks_columns = ["task_id", "subtasks_count"]

# Create the Tasks DataFrame
tasks = spark.createDataFrame(tasks_data, tasks_columns)

# Show the Tasks DataFrame
tasks.show()

# Data for the Executed table
executed_data = [
    (1, 2),
    (3, 1),
    (3, 2),
    (3, 3),
    (3, 4)
]

# Schema for the Executed DataFrame
executed_columns = ["task_id", "subtask_id"]

# Create the Executed DataFrame
executed = spark.createDataFrame(executed_data, executed_columns)

# Show the Executed DataFrame
executed.show()


# COMMAND ----------

tasks.withColumn('subtask_id',explode(sequence(lit(1),col('subtasks_count')))).alias('a')\
    .join(executed.alias('b'), (col('a.task_id')==col('b.task_id'))&(col('a.subtask_id')==col('b.subtask_id')),
          how='left_anti'
          )\
    .select('task_id','subtask_id')\
    .show()

# COMMAND ----------

tasks.createOrReplaceTempView('tasks')
executed.createOrReplaceTempView('executed')

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.task_id, num as subtask_id
# MAGIC from tasks a 
# MAGIC join(
# MAGIC SELECT 1 AS num UNION ALL
# MAGIC     SELECT 2 UNION ALL
# MAGIC     SELECT 3 UNION ALL
# MAGIC     SELECT 4 UNION ALL
# MAGIC     SELECT 5 UNION ALL
# MAGIC     SELECT 6 UNION ALL
# MAGIC     SELECT 7 UNION ALL
# MAGIC     SELECT 8 UNION ALL
# MAGIC     SELECT 9 UNION ALL
# MAGIC     SELECT 10 UNION ALL
# MAGIC     SELECT 11 UNION ALL
# MAGIC     SELECT 12 UNION ALL
# MAGIC     SELECT 13 UNION ALL
# MAGIC     SELECT 14 UNION ALL
# MAGIC     SELECT 15 UNION ALL
# MAGIC     SELECT 16 UNION ALL
# MAGIC     SELECT 17 UNION ALL
# MAGIC     SELECT 18 UNION ALL
# MAGIC     SELECT 19 UNION ALL
# MAGIC     SELECT 20) b
# MAGIC   on a.subtasks_count >= num
# MAGIC left join executed e on a.task_id = e.task_id and e.subtask_id = b.num
# MAGIC where e.task_id is null
# MAGIC order by 1,2

# COMMAND ----------

# MAGIC %md
# MAGIC ###1225. Report Contiguous Dates

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Create Failed and Succeeded DataFrames").getOrCreate()

# Data for the Failed table
failed_data = [
    ("2018-12-28",),
    ("2018-12-29",),
    ("2019-01-04",),
    ("2019-01-05",)
]

# Schema for the Failed DataFrame
failed_columns = ["fail_date"]

# Create the Failed DataFrame
failed = spark.createDataFrame(failed_data, failed_columns)

# Show the Failed DataFrame
failed.show()

# Data for the Succeeded table
succeeded_data = [
    ("2018-12-30",),
    ("2018-12-31",),
    ("2019-01-01",),
    ("2019-01-02",),
    ("2019-01-03",),
    ("2019-01-06",)
]

# Schema for the Succeeded DataFrame
succeeded_columns = ["success_date"]

# Create the Succeeded DataFrame
succeeded = spark.createDataFrame(succeeded_data, succeeded_columns)

# Show the Succeeded DataFrame
succeeded.show()


# COMMAND ----------

failed.withColumn('status',lit('Failed')).unionAll(succeeded.withColumn('status',lit('Succeeded')))\
    .withColumnRenamed('fail_date','date')\
    .filter(col('date').between('2019-01-01', '2019-12-31'))\
    .withColumn('diff',
                row_number().over(Window.orderBy('date'))-dense_rank().over(Window.partitionBy('status').orderBy('date')))     \
    .groupBy('status','diff').agg(min('date').alias('start'),max('date').alias('end'))                \
    .select('status','start','end')\
    .orderBy('start')\
    .show()

# COMMAND ----------

failed.createOrReplaceTempView('failed')
succeeded.createOrReplaceTempView('succeeded')

# COMMAND ----------

# MAGIC %sql
# MAGIC select status, min(date) as start, max(date) as end from(
# MAGIC     select * , row_number() over(order by date) - dense_rank() over(partition by status order by date) as diff
# MAGIC     from(
# MAGIC     select fail_date as date, 'Failed' as status
# MAGIC     from failed
# MAGIC     union all
# MAGIC     select success_date as date, 'Succeeded'
# MAGIC     from succeeded)
# MAGIC     where date between '2019-01-01' and '2019-12-31'
# MAGIC     order by 1,2)
# MAGIC group by status, diff
# MAGIC order by start    

# COMMAND ----------

# MAGIC %sql
# MAGIC select status, min(date) as start, max(date) as end from
# MAGIC (  select *, sum(group_change) over(order by date) as group from
# MAGIC   (  select *, case when prev is null then 1 when prev = status then 0 else 1 end as group_change from
# MAGIC     (  select * , lag(status) over(order by date) as prev
# MAGIC       from(
# MAGIC       select fail_date as date, 'Failed' as status
# MAGIC       from failed
# MAGIC       union all
# MAGIC       select success_date as date, 'Succeeded'
# MAGIC       from succeeded)
# MAGIC       where date between '2019-01-01' and '2019-12-31'
# MAGIC       order by 1,2
# MAGIC     )
# MAGIC   )
# MAGIC )
# MAGIC group by status, group
# MAGIC order by start
