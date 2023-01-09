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

# MAGIC %md ### Set variables & params

# COMMAND ----------

# Set variables

container_source = "raw"
container_destination = "enriched"
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

# Check for duplicates
def contains_duplicates(df):
    if df.count() > df.distinct().count():
        print(f'{table} contains duplicates!')
    else:
        print(f'All good, {table} has no duplicates')
        
# Calculate distance between two points (using the great-circle distance)
def dist(long_x, lat_x, long_y, lat_y):
    return acos(
        sin(toRadians(lat_x)) * sin(toRadians(lat_y)) + 
        cos(toRadians(lat_x)) * cos(toRadians(lat_y)) * 
            cos(toRadians(long_x) - toRadians(long_y))
    ) * lit(6371.0)

# Match city names
def convert_city(df):
    df = df.withColumn("city", when(lower(col("city")).like("%amsterdam%"), "Amsterdam")
                     .when(lower(col("city")).like("%utrecht%"), "Utrecht")
                     .when(lower(col("city")).like("%rotterdam%"), "Rotterdam")
                     .when(lower(col("city")).like("%eindhoven%"), "Eindhoven")
                     .when(lower(col("city")).like("%den haag%"), "Den Haag")
                     .when(lower(col("city")).like("%denhaag%"), "Den Haag")
                     .when(lower(col("city")).like("%groningen%"), "Groningen")
                     .when(lower(col("city")).like("%köln%"), "Köln")
                     .when(lower(col("city")).like("%koln%"), "Köln")
                     .when(lower(col("city")).like("%antwerpen%"), "Antwerpen")
                     .when(lower(col("city")).like("%gent%"), "Gent")
                     .when(lower(col("city")).like("%amersfoort%"), "Amersfoort")
                     .otherwise(df.city))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load from raw, transform and write to enriched

# COMMAND ----------

# External data
table_external = "cities"
df_cities = spark.read.format("csv").load(f"/mnt/raw/external_data/{table_external}/nl.csv", delimiter=";", header=True)

# Set dtypes
df_cities = df_cities.select("city", col("lat2").cast("float"), col("lon2").cast("float"), "country", col("population_urban").cast("int"), col("population_municipal").cast("int"))

# Remove duplicates
df_c = df_cities.toPandas() # convert to pandas (easier to remove duplicates)
df_c = df_c[df_c['population_urban'].notna()] # remove all records that don't have a population
df_c = df_c.drop_duplicates(subset='city', keep="first")
df_cities=spark.createDataFrame(df_c)

# For each object (internal data) in ALDS, load and write as parquet to enriched. For groups and events, do some additional transformations
for table in tables:
    df = spark.read.format("json").load(f"/mnt/raw/data/{table}/{table}.json")
    
    if table == "users":
        # Transformations
        df_fact_groups = df.select("user_id", explode("memberships").alias("struct")).select("user_id", "struct.*") # Create fact table: when users joined a group
        df_fact_groups = unix_datetime(df_fact_groups, "joined") # Convert unix cols to datetime
        df = convert_city(df)
        
        # Enrich with external data
        df = df.alias("df")
        df_cities = df_cities.alias("df_cities")
        df = df.join(df_cities, df.city ==  df_cities.city, "left").select("df.*", "df_cities.lat2", "df_cities.lon2")
        df = df.select("user_id", "city", "country", "hometown", "lat2", "lon2") # base users: reorder columns and drop nested array
        
        # Check for duplicates
        contains_duplicates(df)
        
        # Write to data lake
        df_fact_groups.write.mode("overwrite").parquet(f"/mnt/enriched/data/fact_groups")
        df.write.mode("overwrite").parquet(f"/mnt/enriched/data/dim_{table}")
        
    elif table == "events":
        # Transformations
        df = df.select(md5(concat_ws("", "group_id", "name", "created", "time")).alias("event_id"), "*") # Add id column based on group_id and event name and created
        df_fact_events = df.select("event_id", "group_id", "venue_id", explode("rsvps").alias("struct")).select("event_id", "group_id", "venue_id", "struct.*") # Create fact table: rsvps for events
        df_fact_events = unix_datetime(df_fact_events, "when") # Convert unix cols to datetime
        df = unix_datetime(df, ["created", "time"]) # base events: unix cols to datetime
        df = df.select("event_id", "name", "description", "group_id", "venue_id", "status", "duration", "rsvp_limit", "time", "created") # reorder columns and drop nested array
        df = df.dropDuplicates() # Drop at the end in case base events record contains different rsvps
        
        # Enrich events fact table with proximity (by using venues and user city)
        df_users = spark.read.format("parquet").load(f"/mnt/enriched/data/dim_users/*.parquet")
        df_venues = spark.read.format("parquet").load(f"/mnt/enriched/data/dim_venues/*.parquet")

        df_fact_events = df_fact_events.alias("df_fact_events")
        df_venues = df_venues.alias("df_venues")
        df_users = df_users.alias("df_users")

        df_fact_events = df_fact_events.join(df_venues, df_fact_events.venue_id ==  df_venues.venue_id, "left").select("df_fact_events.*", "df_venues.lat", "df_venues.lon")
        df_fact_events = df_fact_events.alias("df_fact_events")
        df_fact_events = df_fact_events.join(df_users, df_fact_events.user_id ==  df_users.user_id, "left").select("df_fact_events.*", "df_users.lat2", "df_users.lon2")
        df_fact_events = df_fact_events.withColumn("dist", dist("lon", "lat","lon2", "lat2"))
        df_fact_events = df_fact_events.drop(*["lat", "lat2", "lon", "lon2"])        

        # Check for duplicates
        contains_duplicates(df)
        
        # Write to data lake
        df_fact_events.write.mode("overwrite").parquet(f"/mnt/enriched/data/fact_events")
        df.write.mode("overwrite").parquet(f"/mnt/enriched/data/dim_{table}")
        
    elif table == "groups":
        # Tranformations
        df_topics = df.select("group_id", explode("topics").alias("topic")) # Create table containing topics for each group
        df = unix_datetime(df, "created") # base groups: unix cols to datetime
        df = convert_city(df)
        df = df.select("group_id", "name", "description", "city", "lat", "lon", "link", "created") # reorder columns and drop nested array
        
        # Check for duplicates
        contains_duplicates(df)
        
        # Write to data lake
        df_topics.write.mode("overwrite").parquet(f"/mnt/enriched/data/dim_topics")
        df.write.mode("overwrite").parquet(f"/mnt/enriched/data/dim_{table}")
        
    elif table == "venues":
        # Transformations
        df = convert_city(df)
        df = df.select("venue_id", "name", "city", "country", "lat", "lon") # reorder columns
        
        # Check for duplicates
        contains_duplicates(df)
        
        df.write.mode("overwrite").parquet(f"/mnt/enriched/data/dim_{table}")
