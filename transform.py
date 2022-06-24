# Databricks notebook source
# MAGIC %md
# MAGIC ### Configuration

# COMMAND ----------

spark.conf.set("fs.azure.account.key.adlstdmeetup.dfs.core.windows.net", "54KYbLx4u1h/tkbOlAGfamyLtUFgQFLvqPVKm5vlr0rSNUgyelx5gsGM6lIgLyqSedwVFV2zPtTC+AStqSoYBQ==")
# dbutils.fs.ls("abfss://raw@adlstdmeetup.dfs.core.windows.net/data")

# COMMAND ----------

# Set variables

container_source = 'raw'
container_destination = 'enriched'
storage_account = 'adlstdmeetup'

# COMMAND ----------

# Mount /mnt/raw to raw container
dbutils.fs.mount(
  source = f"wasbs://{container_source}@{storage_account}.blob.core.windows.net",
  mount_point = "/mnt/raw",
  extra_configs = {f"fs.azure.account.key.{storage_account}.blob.core.windows.net":"54KYbLx4u1h/tkbOlAGfamyLtUFgQFLvqPVKm5vlr0rSNUgyelx5gsGM6lIgLyqSedwVFV2zPtTC+AStqSoYBQ=="})

# Mount /mnt/enriched to enriched container
dbutils.fs.mount(
  source = f"wasbs://{container_destination}@{storage_account}.blob.core.windows.net",
  mount_point = "/mnt/enriched",
  extra_configs = {f"fs.azure.account.key.{storage_account}.blob.core.windows.net":"54KYbLx4u1h/tkbOlAGfamyLtUFgQFLvqPVKm5vlr0rSNUgyelx5gsGM6lIgLyqSedwVFV2zPtTC+AStqSoYBQ=="})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load data from raw

# COMMAND ----------

table = "users"
df = spark.read.format("json").load(f"/mnt/data/data/{table}/{table}.json")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to enriched

# COMMAND ----------

df.write.json(f"/mnt/enriched/data/{object}.json")
