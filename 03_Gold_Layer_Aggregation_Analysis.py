# 📒 Notebook 03 – Gold Layer: Aggregation & Analysis

from pyspark.sql.functions import *
from pyspark.sql.types import *

# ✅ Step 1: Read from Silver Layer
silver_path = "/mnt/silver/ecommerce_data"
df_silver = spark.read.parquet(silver_path)

# ✅ Step 2: Basic Aggregations

# 1. Count of each event type
df_events_count = df_silver.groupBy("event_type").count()

# 2. Total sales per day
df_sales_per_day = df_silver \
    .filter(col("event_type") == "purchase") \
    .groupBy(to_date("event_time").alias("date")) \
    .agg(sum("price").alias("total_sales"))

# 3. Top purchased products
df_top_products = df_silver \
    .filter(col("event_type") == "purchase") \
    .groupBy("product_id") \
    .agg(count("*").alias("purchase_count")) \
    .orderBy(desc("purchase_count"))

# 4. Top spending users
df_top_users = df_silver \
    .filter(col("event_type") == "purchase") \
    .groupBy("user_id") \
    .agg(sum("price").alias("total_spent")) \
    .orderBy(desc("total_spent"))

# ✅ Step 3: Write to Gold Layer in Delta Format
df_events_count.write.mode("overwrite").format("delta").save("/mnt/gold/events_count")
df_sales_per_day.write.mode("overwrite").format("delta").save("/mnt/gold/sales_per_day")
df_top_products.write.mode("overwrite").format("delta").save("/mnt/gold/top_products")
df_top_users.write.mode("overwrite").format("delta").save("/mnt/gold/top_users")

print("✅ Gold layer data saved successfully.")
