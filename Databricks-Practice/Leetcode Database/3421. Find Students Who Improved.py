# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('leetcode').getOrCreate()

# COMMAND ----------

data = [
    [101, "Math", 70, "2023-01-15"],
    [101, "Math", 85, "2023-02-15"],
    [101, "Physics", 65, "2023-01-15"],
    [101, "Physics", 60, "2023-02-15"],
    [102, "Math", 80, "2023-01-15"],
    [102, "Math", 85, "2023-02-15"],
    [103, "Math", 90, "2023-01-15"],
    [104, "Physics", 75, "2023-01-15"],
    [104, "Physics", 85, "2023-02-15"],
]

schema = ['student_id' , 'subject' , 'score' , 'exam_date']

# COMMAND ----------

scores = spark.createDataFrame(data, schema)
scores.show()

# COMMAND ----------

scores.groupBy('student_id','subject').agg(
    min('exam_date').alias('first_exam_date'),
    max('exam_date').alias('last_exam_date')
).alias('a')\
.join(scores.alias('b'), (col('a.student_id')==col('b.student_id')) & (col('a.subject')==col('b.subject')) & (col('a.first_exam_date')==col('b.exam_date'))
)\
.join(scores.alias('c'), (col('a.student_id')==col('c.student_id')) & (col('a.subject')==col('c.subject')) & (col('a.last_exam_date')==col('c.exam_date'))
)\
.select(col('a.student_id'),col('a.subject'),col('b.score').alias('first_score'), col('c.score').alias('latest_score'))\
.filter(col('latest_score')>col('first_score'))\
.orderBy('student_id','subject')\
.show(truncate=False)  



