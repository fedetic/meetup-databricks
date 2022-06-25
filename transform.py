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

# Set variables

container_source = "raw"
container_destination = "enriched"
storage_account = "adlstdmeetup"
tables = ["groups", "users", "events", "venues"]

# COMMAND ----------

# # Only needed in first run
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

# MAGIC %md
# MAGIC ### Load from raw, transform and write to enriched

# COMMAND ----------

# For each object in ALDS, load and write as parquet to enriched. For groups and events, do some additional transformations
for table in tables:
    df = spark.read.format("json").load(f"/mnt/raw/data/{table}/{table}.json")
    
    if table == 'users':
        df_fact_groups = df.select("user_id", explode("memberships").alias("struct")).select("user_id", "struct.*")
        
        df_fact_groups.write.mode("overwrite").parquet(f"/mnt/enriched/data/fact_groups")
        df.write.mode("overwrite").parquet(f"/mnt/enriched/data/{table}")
        
    elif table == 'events':
        df_fact_events = df.select(md5(concat("group_id", "name")).alias("event_id"), explode("rsvps").alias("struct")).select("event_id", "struct.*")
        
        df_fact_events.write.mode("overwrite").parquet(f"/mnt/enriched/data/fact_events")
        df.write.mode("overwrite").parquet(f"/mnt/enriched/data/{table}")
        
    else:
        df.write.mode("overwrite").parquet(f"/mnt/enriched/data/{table}")
