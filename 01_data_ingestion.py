# Databricks notebook source
# MAGIC %md
# # 📥 Data Ingestion - E-Commerce Dataset
# 
# هذا الـ Notebook يقوم بتحميل بيانات الـ E-Commerce من ملف ZIP داخل Azure Data Lake، واستخراجها ودمجها، ثم حفظها في Bronze Layer.

# COMMAND ----------

# 📌 إعدادات الاتصال - باستخدام Databricks Secrets (أفضل ممارسة)
storage_account_name = "ecomintelstorage"
container_name = "raw"

configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="ecom-scope", key="client-id"),
  "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="ecom-scope", key="client-secret"),
  "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='ecom-scope', key='tenant-id')}/oauth2/token"
}

# Mount ADLS Gen2 to DBFS
mount_point = f"/mnt/{container_name}"
if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(mount_point)

dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)

# COMMAND ----------

# عرض الملفات داخل الحاوية
display(dbutils.fs.ls("/mnt/
