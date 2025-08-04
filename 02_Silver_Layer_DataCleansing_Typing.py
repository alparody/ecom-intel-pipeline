# Notebook 02 – Silver Layer: Data Cleansing & Typing

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import LongType, DoubleType, StringType, TimestampType

# ⛽ Start Spark Session
spark = SparkSession.builder.getOrCreate()

# 📥 Read from Bronze Layer (CSV files)
bronze_path = "/mnt/bronze/ecommerce_data"
all_files = dbutils.fs.ls(bronze_path)

# استخراج ملفات CSV فقط وحجمها أكبر من صفر
valid_csv_files = [f.path for f in all_files if f.path.endswith(".csv") and f.size > 0]

# قراءة البيانات من الملفات الصالحة
df_bronze = spark.read.option("header", True).csv(valid_csv_files)

print(f"✅ عدد الصفوف قبل التنظيف: {df_bronze.count()}")

# 🧽 تنظيف وتحويل أنواع البيانات
df_silver = df_bronze \
    .withColumn("event_time", col("event_time").cast(TimestampType())) \
    .withColumn("event_type", col("event_type").cast(StringType())) \
    .withColumn("product_id", col("product_id").cast(LongType())) \
    .withColumn("category_id", col("category_id").cast(LongType())) \
    .withColumn("category_code", col("category_code").cast(StringType())) \
    .withColumn("brand", col("brand").cast(StringType())) \
    .withColumn("price", col("price").cast(DoubleType())) \
    .withColumn("user_id", col("user_id").cast(LongType())) \
    .withColumn("user_session", col("user_session").cast(StringType()))

# حذف الصفوف التي تحتوي على قيم Null حرجة
df_silver_cleaned = df_silver.dropna(subset=["event_time", "event_type", "product_id", "price"])

print(f"✅ عدد الصفوف بعد التنظيف: {df_silver_cleaned.count()}")

# 📤 Save to Silver Layer as Parquet
silver_path = "/mnt/silver/ecommerce_data"

df_silver_cleaned.write.mode("overwrite").parquet(silver_path)

print("✅ تم حفظ البيانات في Silver Layer بصيغة Parquet")
