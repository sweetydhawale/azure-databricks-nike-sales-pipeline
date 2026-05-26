# Databricks notebook source
from pyspark.sql import functions as F

silver_path = "/Volumes/dbw_nike_dev/default/silver/nike_sales_silver"

silver_df = spark.read.format("delta").load(silver_path)

silver_df.show(5)

# COMMAND ----------

product_sales = silver_df.groupBy("Product_Line") \
    .agg(
        F.sum("Revenue").alias("Total_Revenue"),
        F.sum("Profit").alias("Total_Profit"),
        F.sum("Units_Sold").alias("Total_Units_Sold")
    )

product_sales.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/Volumes/dbw_nike_dev/default/gold/product_sales_gold")

product_sales.show()

# COMMAND ----------

region_sales = silver_df.groupBy("Region") \
    .agg(
        F.sum("Revenue").alias("Total_Revenue"),
        F.sum("Profit").alias("Total_Profit"),
        F.sum("Units_Sold").alias("Total_Units_Sold")
    )

region_sales.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/Volumes/dbw_nike_dev/default/gold/region_sales_gold")

region_sales.show()

# COMMAND ----------

channel_sales = silver_df.groupBy("Sales_Channel") \
    .agg(
        F.sum("Revenue").alias("Total_Revenue"),
        F.sum("Profit").alias("Total_Profit"),
        F.count("Order_ID").alias("Total_Orders")
    )

channel_sales.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/Volumes/dbw_nike_dev/default/gold/channel_sales_gold")

channel_sales.show()