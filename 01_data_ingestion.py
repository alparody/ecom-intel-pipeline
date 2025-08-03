# Databricks notebook source
# ⚠️ هذه الخلية فقط للاستخدام المحلي، لا ترفعها على GitHub
# client_id = "xxx"
# tenant_id = "xxx"
# client_secret = "xxx"

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": client_id,
  "fs.azure.account.oauth2.client.secret": client_secret,
  "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

storage_account_name = "ecomintelstorage"
container_name = "raw"

# إعادة mount للتأكد من الاتصال
try:
    dbutils.fs.unmount(f"/mnt/{container_name}")
except:
    pass

dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{container_name}",
  extra_configs = configs)

# COMMAND ----------

# عرض الملفات داخل الـ container
display(dbutils.fs.ls("/mnt/raw"))

# COMMAND ----------

# تحميل ملف الـ ZIP إلى المسار المحلي
local_zip_path = "/tmp/EcommerceDataSet.zip"
dbutils.fs.cp("dbfs:/mnt/raw/EcommerceDataSet.zip", f"file:{local_zip_path}")

# COMMAND ----------

# فك الضغط
import zipfile, os

extract_path = "/tmp/ecommerce_extract"
os.makedirs(extract_path, exist_ok=True)

with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
    zip_ref.extract_
