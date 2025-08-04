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

