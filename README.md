# 🛒 Real-Time E-Commerce Analytics – Bronze & Silver Layers

مشروع عملي باستخدام **Azure Databricks** و**Delta Lake** لتحليل بيانات التجارة الإلكترونية في الزمن الحقيقي.  
يبدأ المشروع من طبقة Bronze بتحميل البيانات الأولية، ثم تنظيفها وتحويلها في Silver Layer.

---

## 🗂️ Project Structure

├── 01_bronze_layer_ingestion.py # تحميل البيانات إلى Bronze Layer
├── 02_silver_layer_cleaning.py # تنظيف البيانات وتحويلها إلى Silver Layer
├── README.md # توثيق المشروع


---

## 🧱 Notebook 01 – Bronze Layer: Ingestion

### 🎯 الهدف:
تحميل ملفات CSV من المصدر الخام وكتابتها في Bronze Layer على Databricks (DBFS أو ADLS).

### ✅ الخطوات:

1. **رفع البيانات إلى DBFS:**
   باستخدام كود Python لنقل ملفات CSV إلى `/dbfs/tmp/`.

2. **قراءة الملفات ودمجها:**

   df = spark.read.option("header", "true").csv("dbfs:/tmp/ecommerce_extract/*.csv")

3. **كتابة البيانات في Bronze Layer:**
df.write.mode("overwrite").csv("dbfs:/mnt/bronze/ecommerce_data")

✅ النتيجة:

ملفات CSV محفوظة كما هي

بدون تنظيف أو تعديل على الأنواع

قابلة للقراءة لاحقًا في Silver Layer
🪞 Notebook 02 – Silver Layer: Cleansing & Typing

🎯 الهدف:
تنظيف البيانات الخام، معالجة الأنواع، وكتابة نسخة نظيفة بصيغة Parquet.

✅ الخطوات:
1- قراءة ملفات CSV من Bronze بعد التأكد من الحجم:
valid_csv_files = [f.path for f in all_files if f.path.endswith(".csv") and f.size > 0]
df_bronze = spark.read.option("header", True).csv(valid_csv_files)

2- تحويل أنواع البيانات:
from pyspark.sql.types import *

df = df \
    .withColumn("event_time", col("event_time").cast(TimestampType())) \
    .withColumn("product_id", col("product_id").cast(LongType())) \
    # تابع التحويل لباقي الأعمدة حسب الحاجة

3- تنظيف القيم الفارغة:
حذف الصفوف التي تحتوي على Null في أعمدة حرجة:
df_cleaned = df.dropna(subset=["event_time", "event_type", "product_id", "price"])

4- كتابة البيانات في Silver Layer بصيغة Parquet:
df_cleaned.write.mode("overwrite").parquet("/mnt/silver/ecommerce_data")

📊 مقارنة طبقات المعالجة
Layer	  Format	Cleaned	Typed	Null Removed	Ready for Analysis
Bronze	CSV	    ❌	    ❌	       ❌	      ❌
Silver	Parquet	✅	    ✅	       ✅	      ✅


# 🟡 Gold Layer – Real-Time E-Commerce Analytics

تهدف هذه المرحلة إلى إنتاج جداول تحليلية جاهزة للاستخدام في لوحات البيانات (BI Dashboards).

---

## 🎯 الهدف من Gold Layer

تحويل البيانات النظيفة (Silver Layer) إلى رؤى تحليلية قابلة للاستخدام، مثل:

- تتبع سلوك المستخدم (event funnel)
- تحليل المبيعات حسب الوقت، المنتج، والمستخدم
- تحديد أعلى المنتجات مبيعًا وأكثر المستخدمين إنفاقًا

---

## 📁 Notebook 03 – Gold Layer: Aggregation & Analysis

### ✅ الخطوات:

1. **قراءة البيانات من Silver Layer (Parquet):**

```python
silver_path = "/mnt/silver/ecommerce_data"
df_silver = spark.read.parquet(silver_path)
تنفيذ التحليلات:

🕒 عدد الأحداث حسب نوع الحدث:
df_events_count = df_silver.groupBy("event_type").count()
💰 إجمالي المبيعات اليومية:
df_sales_per_day = df_silver \
    .filter(col("event_type") == "purchase") \
    .groupBy(to_date("event_time").alias("date")) \
    .agg(sum("price").alias("total_sales"))
🛍️ المنتجات الأكثر مبيعًا:
df_top_products = df_silver \
    .filter(col("event_type") == "purchase") \
    .groupBy("product_id") \
    .agg(count("*").alias("purchase_count")) \
    .orderBy(desc("purchase_count"))
👤 أعلى المستخدمين إنفاقًا:
df_top_users = df_silver \
    .filter(col("event_type") == "purchase") \
    .groupBy("user_id") \
    .agg(sum("price").alias("total_spent")) \
    .orderBy(desc("total_spent"))
كتابة النتائج إلى Gold Layer بصيغة Delta:

df_top_products.write.mode("overwrite").format("delta").save("/mnt/gold/top_products")
df_sales_per_day.write.mode("overwrite").format("delta").save("/mnt/gold/sales_per_day")
df_top_users.write.mode("overwrite").format("delta").save("/mnt/gold/top_users")
🧾 النتائج المخزنة
Folder	التحليل
/mnt/gold/top_products	المنتجات الأكثر مبيعًا
/mnt/gold/sales_per_day	إجمالي المبيعات اليومية
/mnt/gold/top_users	المستخدمين الأعلى إنفاقًا

📌 ملاحظات:
تم تخزين النتائج بصيغة Delta Lake لدعم Time Travel وعمليات الـ ACID.

التحليلات قابلة للتوسعة حسب الحاجة (مثل التحليل الجغرافي أو تحليلات الـ Funnel).
