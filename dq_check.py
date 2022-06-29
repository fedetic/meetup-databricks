# Databricks notebook source
# MAGIC %md
# MAGIC ### Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark.conf.set("fs.azure.account.key.adlstdmeetup.dfs.core.windows.net", "54KYbLx4u1h/tkbOlAGfamyLtUFgQFLvqPVKm5vlr0rSNUgyelx5gsGM6lIgLyqSedwVFV2zPtTC+AStqSoYBQ==")
# dbutils.fs.ls("abfss://raw@adlstdmeetup.dfs.core.windows.net/data")

# COMMAND ----------

 # Only needed in first run
# # Mount /mnt/raw to raw container
# dbutils.fs.mount(
#   source = f"wasbs://{container_source}@{storage_account}.blob.core.windows.net",
#   mount_point = "/mnt/raw",
#   extra_configs = {f"fs.azure.account.key.{storage_account}.blob.core.windows.net":"54KYbLx4u1h/tkbOlAGfamyLtUFgQFLvqPVKm5vlr0rSNUgyelx5gsGM6lIgLyqSedwVFV2zPtTC+AStqSoYBQ=="})

# # Mount /mnt/enriched to enriched container
# dbutils.fs.mount(
#   source = f"wasbs://{container_destination}@{storage_account}.blob.core.windows.net",
#   mount_point = "/mnt/enriched",
#   extra_configs = {f"fs.azure.account.key.{storage_account}.blob.core.windows.net":"54KYbLx4u1h/tkbOlAGfamyLtUFgQFLvqPVKm5vlr0rSNUgyelx5gsGM6lIgLyqSedwVFV2zPtTC+AStqSoYBQ=="})

# COMMAND ----------

# MAGIC %md ### Set variables & params

# COMMAND ----------

# Set variables

container_source = "raw"
storage_account = "adlstdmeetup"
tables = ["groups", "users", "venues", "events"]

# COMMAND ----------

# Create parameter - if we only want to run this script for a subset of tables

dbutils.widgets.text("input", "","")
y = dbutils.widgets.get("input").split(',')
if len(y) > 0 and y[0] != '':
    tables = y

print(tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Small data catalog

# COMMAND ----------

# External data
table_external = "cities"
df_cities = spark.read.format("csv").load(f"/mnt/raw/external_data/{table_external}/nl.csv", delimiter=";", header=True)
display(df_cities)

# COMMAND ----------

for table in tables:
    df = spark.read.format("json").load(f"/mnt/raw/data/{table}/{table}.json")
    print(table)
    display(df)
