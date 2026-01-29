# Databricks notebook source
display(spark.sql("SHOW VOLUMES"))

# COMMAND ----------

# MAGIC %md
# MAGIC # **Create a catalog named 'db_retail'.**

# COMMAND ----------

# DBTITLE 1,Create managed volume without LOCATION clause

catalog_name ='db_retail'  # variable to hold catalog name
volume_name = 'v01.retail_pipeline.customers.stream_json'  #variable to hold volume name
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}") # creates catalog



# COMMAND ----------

spark.sql("""
CREATE VOLUME IF NOT EXISTS db_retail.v01.retail_pipeline
""")  # create a volume retail_pipeline


# COMMAND ----------

# DBTITLE 1,Copy first 2 files between volumes
# following code is basically for moving the source data to the destination. The dblearner will hold the source data in the project.
from pyspark.dbutils import DBUtils
import os

dbutils = DBUtils(spark)

src_base_dir = '/Volumes/db_retail/v01/retail_pipeline'
dst_base_dir = '/Volumes/learnwithatul/ops/dblearner'

folders = ['customers/stream_json', 'orders/stream_json', 'status/stream_json']

for folder in folders:
    src_dir = os.path.join(src_base_dir, folder)
    dst_dir = os.path.join(dst_base_dir, folder.split('/')[0])
    files = dbutils.fs.ls(src_dir)
    if files:
        dbutils.fs.cp(files[0].path, os.path.join(dst_dir, os.path.basename(files[0].path)))