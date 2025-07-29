# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('leetcode').getOrCreate()

# COMMAND ----------

schema = ["sample_id", "dna_sequence", "species"]

data = [
    (1, "ATGCTAGCTAGCTAA", "Human"),
    (2, "GGGTCAATCATC", "Human"),
    (3, "ATATATCGTAGCTA", "Human"),
    (4, "ATGGGGTCATCATAA", "Mouse"),
    (5, "TCAGTCAGTCAG", "Mouse"),
    (6, "ATATCGCGCTAG", "Zebrafish"),
    (7, "CGTATGCGTCGTA", "Zebrafish")
]


# COMMAND ----------

samples = spark.createDataFrame(data, schema)
samples.show()

# COMMAND ----------

samples.withColumns({
    'has_start':when(col('dna_sequence').startswith('ATG'),1).otherwise(0),
    'has_stop':when(col('dna_sequence').rlike(r'.*(TAA|TAG|TGA)$'),1).otherwise(0),
    'has_atat':when(col('dna_sequence').like(r'%ATAT%'),1).otherwise(0),
    'has_ggg':when(col('dna_sequence').like(r'%GGG%'),1).otherwise(0)
}).show()
