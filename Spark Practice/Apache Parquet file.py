# Databricks notebook source
df = spark.read.parquet("dbfs:/FileStore/PARQUET/part_r_00000_1a9822ba_b8fb_4d8e_844a_ea30d0801b9e_gz.parquet")
df.show()
