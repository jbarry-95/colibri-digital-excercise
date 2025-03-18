# Databricks notebook source
# DBTITLE 1,initial config setup
# my_catalog = "main"
# my_schema = "renewable_energy"
# my_volume = "re_vol"

# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {my_catalog}.{my_schema}")
# spark.sql(f"CREATE VOLUME IF NOT EXISTS {my_catalog}.{my_schema}.{my_volume}")

# volume_path = f"/Volumes/{my_catalog}/{my_schema}/{my_volume}/"



# COMMAND ----------

# DBTITLE 1,import and variable definition
from pyspark.sql.functions import min, max, avg, col, stddev, mean, window, to_timestamp
from pyspark.sql import functions as F
from delta.tables import *
import os

my_catalog = "main" #os.getenv("my_catalog")
my_schema = "renewable_energy" #os.getenv("my_schema")
my_volume = "re_vol" #os.getenv("my_volume")

volume_path = f"/Volumes/{my_catalog}/{my_schema}/{my_volume}/"

# Define file paths
file1_path = f"{volume_path}data_group_1.csv"
file2_path = f"{volume_path}data_group_2.csv"
file3_path = f"{volume_path}data_group_3.csv"


# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load(file1_path)
df2 = spark.read.format("csv").option("header", "true").load(file2_path)
df3 = spark.read.format("csv").option("header", "true").load(file3_path)

# COMMAND ----------

# DBTITLE 1,raw data 1
##Check table exists - file 1
if (not spark.catalog.tableExists(f"{my_catalog}.{my_schema}.raw_data_1")):
    df1.write.format("delta").saveAsTable("main.renewable_energy.raw_data_1")
else:
    deltaTable = DeltaTable.forName(spark, "main.renewable_energy.raw_data_1")

    (deltaTable.alias('tbl')
    .merge(
        df1.alias('updates'),
        'tbl.timestamp = updates.timestamp AND tbl.turbine_id = updates.turbine_id'
    ) 
    .whenNotMatchedInsert(values =
        {
        "timestamp": "updates.timestamp",
        "turbine_id": "updates.turbine_id",
        "wind_speed": "updates.wind_speed",
        "wind_direction": "updates.wind_direction",
        "power_output": "updates.power_output"
        }
    ) 
    .execute())

# COMMAND ----------

# DBTITLE 1,raw data 2
##Check table exists - file 2
if (not spark.catalog.tableExists(f"{my_catalog}.{my_schema}.raw_data_2")):
    df2.write.format("delta").saveAsTable("main.renewable_energy.raw_data_2")
else:
    deltaTable = DeltaTable.forName(spark, "main.renewable_energy.raw_data_2")

    (deltaTable.alias('tbl')
    .merge(
        df2.alias('updates'),
        'tbl.timestamp = updates.timestamp AND tbl.turbine_id = updates.turbine_id'
    ) 
    .whenNotMatchedInsert(values =
        {
        "timestamp": "updates.timestamp",
        "turbine_id": "updates.turbine_id",
        "wind_speed": "updates.wind_speed",
        "wind_direction": "updates.wind_direction",
        "power_output": "updates.power_output"
        }
    ) 
    .execute())

# COMMAND ----------

# DBTITLE 1,raw data 3
##Check table exists - file 3
if (not spark.catalog.tableExists(f"{my_catalog}.{my_schema}.raw_data_3")):
    df3.write.format("delta").saveAsTable("main.renewable_energy.raw_data_3")
else:
    deltaTable = DeltaTable.forName(spark, "main.renewable_energy.raw_data_3")

    (deltaTable.alias('tbl')
    .merge(
        df3.alias('updates'),
        'tbl.timestamp = updates.timestamp AND tbl.turbine_id = updates.turbine_id'
    ) 
    .whenNotMatchedInsert(values =
        {
        "timestamp": "updates.timestamp",
        "turbine_id": "updates.turbine_id",
        "wind_speed": "updates.wind_speed",
        "wind_direction": "updates.wind_direction",
        "power_output": "updates.power_output"
        }
    ) 
    .execute())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.renewable_energy.summary_data