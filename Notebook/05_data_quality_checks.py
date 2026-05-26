# Databricks notebook source
from pyspark.sql.functions import col, sum as spark_sum

silver_path = "/Volumes/dbw_nike_dev/default/silver/nike_sales_silver"

silver_df = spark.read.format("delta").load(silver_path)

# COMMAND ----------

print("Silver row count:", silver_df.count())

# COMMAND ----------

dq_nulls = silver_df.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in ["Order_ID", "Product_Line", "Revenue", "Order_Date", "Region"]
])

dq_nulls.show()

# COMMAND ----------

duplicate_orders = silver_df.groupBy("Order_ID") \
    .count() \
    .filter("count > 1")

duplicate_orders.show()

# COMMAND ----------

dq_summary = spark.createDataFrame([
    ("Silver Row Count", silver_df.count()),
    ("Duplicate Orders", duplicate_orders.count())
], ["check_name", "check_value"])

dq_summary.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/Volumes/dbw_nike_dev/default/gold/dq_summary")

dq_summary.show()