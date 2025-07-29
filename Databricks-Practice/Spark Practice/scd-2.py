# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

customer_dim_data = [

(1,'manish','arwal','india','N','2022-09-15','2022-09-25'),
(2,'vikash','patna','india','Y','2023-08-12',None),
(3,'nikita','delhi','india','Y','2023-09-10',None),
(4,'rakesh','jaipur','india','Y','2023-06-10',None),
(5,'ayush','NY','USA','Y','2023-06-10',None),
(1,'manish','gurgaon','india','Y','2022-09-25',None),
]

customer_schema= ['id','name','city','country','active','effective_start_date','effective_end_date']

customer_dim_df = spark.createDataFrame(data= customer_dim_data,schema=customer_schema)

customer_dim_df.orderBy('id',asc('effective_start_date')).show()

# COMMAND ----------

sales_data = [

(1,1,'manish','2023-01-16','gurgaon','india',380),
(77,1,'manish','2023-03-11','bangalore','india',300),
(12,3,'nikita','2023-09-20','delhi','india',127),
(54,4,'rakesh','2023-08-10','jaipur','india',321),
(65,5,'ayush','2023-09-07','mosco','russia',765),
(89,6,'rajat','2023-08-10','jaipur','india',321)
]

sales_schema = ['sales_id', 'customer_id','customer_name', 'sales_date', 'food_delivery_address','food_delivery_country', 'food_cost']

sales_df = spark.createDataFrame(data=sales_data,schema=sales_schema)

sales_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Capturing existing customer with new address

# COMMAND ----------

new_records = customer_dim_df.join(sales_df,customer_dim_df.id==sales_df.customer_id,'inner')\
    .filter((col('active')=='Y') & (col('city')!=col('food_delivery_address')))\
    .selectExpr('id', 'name',
                'food_delivery_address as city', 
                'food_delivery_country as country',
                'active',
                'sales_date as effective_start_date',
                'NULL as effective_start_date')

new_records.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Capturing exisiting customers whose address changed

# COMMAND ----------

old_records = customer_dim_df.join(sales_df,customer_dim_df.id==sales_df.customer_id,'inner')\
    .filter((col('active')=='Y') & (col('city')!=col('food_delivery_address')))\
    .selectExpr('id', 'name',
                'city', 
                'country',
                '"N" as active',
                'effective_start_date',
                'sales_date as effective_end_date')
    
old_records.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Capturing new customers

# COMMAND ----------

new_customer = sales_df.join(customer_dim_df,sales_df.customer_id==customer_dim_df.id, 'left_anti')\
    .selectExpr(
        'customer_id as id',
        'customer_name as name',
        'food_delivery_address as city',
        'food_delivery_country as country',
        '"Y" as active',
        'sales_date as effective_start_date',
        'NULL as effective_end_date'
    )
    
new_customer.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Modifying customer dim table

# COMMAND ----------

from pyspark.sql import Window
window = Window.partitionBy('id','active').orderBy(col('effective_start_date').desc())

# COMMAND ----------

customer_dim_df.union(old_records).union(new_records).union(new_customer)\
    .withColumn('rn',row_number().over(window))\
    .filter(~((col('rn')>=2) & (col('active')=='Y')))\
    .drop('rn')\
    .orderBy('id','effective_start_date')\
    .show()
