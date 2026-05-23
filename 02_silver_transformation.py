# Databricks notebook source
from pyspark.sql.functions import col, round, expr

# Read Bronze data
df_bronze = spark.read.csv(
    "/Volumes/dbw_nike_dev/default/landingzone/Nike_Sales_Uncleaned.csv",
    header=True,
    inferSchema=True
)

# Silver cleaning
df_silver = df_bronze.dropDuplicates(["Order_ID"])

# Cast numeric columns
df_silver = df_silver.withColumn("Units_Sold", col("Units_Sold").cast("double")) \
    .withColumn("MRP", col("MRP").cast("double")) \
    .withColumn("Discount_Applied", col("Discount_Applied").cast("double")) \
    .withColumn("Revenue", col("Revenue").cast("double")) \
    .withColumn("Profit", col("Profit").cast("double"))

# Handle multiple date formats safely
df_silver = df_silver.withColumn(
    "Order_Date",
    expr("""
        to_date(
            coalesce(
                try_to_timestamp(Order_Date, 'yyyy/MM/dd'),
                try_to_timestamp(Order_Date, 'yyyy-MM-dd'),
                try_to_timestamp(Order_Date, 'dd-MM-yyyy')
            )
        )
    """)
)

# Remove records where important fields are still null
df_silver = df_silver.dropna(subset=[
    "Order_ID",
    "Product_Line",
    "Revenue",
    "Order_Date",
    "Region"
])

df_silver = df_silver.filter(
    (col("Units_Sold").isNotNull()) &
    (col("Units_Sold") > 0) &
    (col("MRP").isNotNull()) &
    (col("MRP") > 0) &
    (col("Discount_Applied").isNotNull()) &
    (col("Discount_Applied") >= 0) &
    (col("Discount_Applied") <= 1)
)

# Revenue validation
df_silver = df_silver.withColumn(
    "calculated_revenue",
    round(col("Units_Sold") * col("MRP") * (1 - col("Discount_Applied")), 2)
)

# Save Silver Delta
silver_path = "/Volumes/dbw_nike_dev/default/silver/nike_sales_silver"

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_path)

# Validate
silver_df = spark.read.format("delta").load(silver_path)

silver_df.show(10)
silver_df.printSchema()
print("Silver row count:", silver_df.count())