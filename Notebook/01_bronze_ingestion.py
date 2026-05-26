# Databricks notebook source
##Read raw Nike CSV from landingzone volume.##
##Validate schema and row count.##

bronze_path = "/Volumes/dbw_nike_dev/default/landingzone/Nike_Sales_Uncleaned.csv"

df_bronze = spark.read.csv(
    bronze_path,
    header=True,
    inferSchema=True
)

df_bronze.show(10)
df_bronze.printSchema()
print("Bronze row count:", df_bronze.count())