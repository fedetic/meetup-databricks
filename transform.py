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
container_destination = "enriched"
storage_account = "adlstdmeetup"
tables = ["groups", "users", "events", "venues"]

# COMMAND ----------

# Create parameter - if we only want to run this script for a subset of tables

dbutils.widgets.text("input", "","")
y = dbutils.widgets.get("input").split(',')
if len(y) > 0 and y[0] != '':
    tables = y

print(tables)

# COMMAND ----------

# MAGIC %md ### Create functions

# COMMAND ----------

# Convert unix cols in df to datetime
def unix_datetime(df, cols_unix):
    if type(cols_unix) in (list, tuple):
        for col_unix in cols_unix:
            df = df.withColumn(col_unix, (col(col_unix)/1000).cast(TimestampType()))
    else:
        df = df.withColumn(cols_unix, (col(cols_unix)/1000).cast(TimestampType()))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load from raw, transform and write to enriched

# COMMAND ----------

# For each object in ALDS, load and write as parquet to enriched. For groups and events, do some additional transformations
for table in tables:
    df = spark.read.format("json").load(f"/mnt/raw/data/{table}/{table}.json")
    
    if table == "users":
        # Transformations
        df_fact_groups = df.select("user_id", explode("memberships").alias("struct")).select("user_id", "struct.*") # Create fact table: when users joined a group
        df_fact_groups = unix_datetime(df_fact_groups, "joined") # Convert unix cols to datetime
        df = df.select("user_id", "city", "country", "hometown") # base users: reorder columns and drop nested array
        
        # Write to data lake
        df_fact_groups.write.mode("overwrite").parquet(f"/mnt/enriched/data/fact_groups")
        df.write.mode("overwrite").parquet(f"/mnt/enriched/data/dim_{table}")
        
    elif table == "events":
        # Transformations
        df = df.select(md5(concat("group_id", "name")).alias("event_id"), "*") # Add id column based on group_id and name
        df_fact_events = df.select("event_id", explode("rsvps").alias("struct")).select("event_id", "struct.*") # Create fact table: rsvps for events
        df_fact_events = unix_datetime(df_fact_events, "when") # Convert unix cols to datetime
        df = unix_datetime(df, ["created", "time"]) # base events: unix cols to datetime
        df = df.select("event_id", "name", "description", "group_id", "venue_id", "status", "duration", "rsvp_limit", "time", "created") # reorder columns and drop nested array
        
        # Write to data lake
        df_fact_events.write.mode("overwrite").parquet(f"/mnt/enriched/data/fact_events")
        df.write.mode("overwrite").parquet(f"/mnt/enriched/data/dim_{table}")
        
    elif table == "groups":
        # Tranformations
        df_topics = df.select("group_id", explode("topics").alias("topic")) # Create table containing topics for each group
        df = unix_datetime(df, "created") # base groups: unix cols to datetime
        df = df.select("group_id", "name", "description", "city", "lat", "lon", "link", "created") # reorder columns and drop nested array
        
        # Write to data lake
        df_topics.write.mode("overwrite").parquet(f"/mnt/enriched/data/dim_topics")
        df.write.mode("overwrite").parquet(f"/mnt/enriched/data/dim_{table}")
        
    elif table == "venues":
        df = df.select("venue_id", "name", "city", "country", "lat", "lon") # reorder columns
        df.write.mode("overwrite").parquet(f"/mnt/enriched/data/dim_{table}")
