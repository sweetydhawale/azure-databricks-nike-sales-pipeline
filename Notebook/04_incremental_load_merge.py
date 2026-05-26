# Databricks notebook source
from pyspark.sql import Row
from pyspark.sql.functions import col, round, to_date
from delta.tables import DeltaTable

silver_path = "/Volumes/dbw_nike_dev/default/silver/nike_sales_silver"

# COMMAND ----------

incremental_data = [
    Row(Order_ID=1001, Gender_Category="Men", Product_Line="Shoes", Product_Name="Nike Air Max", Size="9", Units_Sold=2.0, MRP=5000.0, Discount_Applied=0.10, Revenue=9000.0, Order_Date="2025/05/01", Sales_Channel="Online", Region="West", Profit=1500.0),
    Row(Order_ID=1002, Gender_Category="Women", Product_Line="Apparel", Product_Name="Nike Hoodie", Size="M", Units_Sold=1.0, MRP=3000.0, Discount_Applied=0.05, Revenue=2850.0, Order_Date="2025/05/02", Sales_Channel="Store", Region="South", Profit=700.0)
]

df_incremental = spark.createDataFrame(incremental_data)

# COMMAND ----------

df_incremental_clean = df_incremental.dropDuplicates().dropna()

df_incremental_clean = df_incremental_clean.withColumn("Units_Sold", col("Units_Sold").cast("double")) \
    .withColumn("MRP", col("MRP").cast("double")) \
    .withColumn("Discount_Applied", col("Discount_Applied").cast("double")) \
    .withColumn("Order_Date", to_date("Order_Date", "yyyy/MM/dd")) \
    .withColumn("calculated_revenue", round(col("Units_Sold") * col("MRP") * (1 - col("Discount_Applied")), 2))

# COMMAND ----------

silver_delta = DeltaTable.forPath(spark, silver_path)

silver_delta.alias("target") \
    .merge(
        df_incremental_clean.alias("source"),
        "target.Order_ID = source.Order_ID"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# COMMAND ----------

silver_after_merge = spark.read.format("delta").load(silver_path)
silver_after_merge.filter("Order_ID IN (1001, 1002)").show()