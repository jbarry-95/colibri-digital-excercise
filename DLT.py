# Databricks notebook source
# Import modules

import dlt
from pyspark.sql.functions import min, max, avg, col, stddev, mean, window, to_timestamp

# COMMAND ----------

@dlt.table()
@dlt.expect_or_drop("valid_timstamp", "timestamp IS NOT NULL")
@dlt.expect_or_drop("valid_turbine_id", "turbine_id IS NOT NULL")
@dlt.expect_or_drop("valid_wind_speed", "wind_speed IS NOT NULL")
@dlt.expect_or_drop("valid_wind_direction", "wind_direction IS NOT NULL")
@dlt.expect_or_drop("valid_power_output", "power_output IS NOT NULL")
def clean_data():
    
    raw_1 = (spark.table("main.renewable_energy.raw_data_1")
                                    .withColumn("timestamp", to_timestamp(col("timestamp"), "dd/MM/yyyy HH:mm"))
        )

    raw_2 = (spark.table("main.renewable_energy.raw_data_2")
                                    .withColumn("timestamp", to_timestamp(col("timestamp"), "dd/MM/yyyy HH:mm"))
        )
        
    raw_3 = (spark.table("main.renewable_energy.raw_data_3")
                                    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
        )

    raw_data = (raw_1.union(raw_2).union(raw_3))

    return (raw_data)

# COMMAND ----------

@dlt.table()
def summary_data():
    
    power_output_stats = (spark.table("main.renewable_energy.clean_data")
    .groupBy("turbine_id", window(col("timestamp"), "24 hours"))
    .agg(
        min("power_output").alias("min_power_output"),
        max("power_output").alias("max_power_output"),
        avg("power_output").alias("avg_power_output"),
        stddev("power_output").alias("stddev_power_output")
    )
)
    return (power_output_stats)

# COMMAND ----------

@dlt.table()
def anomaly_data():
    stats = spark.table("main.renewable_energy.summary_data")

    # Join the stats with the raw data
    clean_data_with_stats = spark.table("main.renewable_energy.clean_data") \
        .join(stats, 
            (col("raw_data.turbine_id") == col("summary_data.turbine_id")) & 
            ((col("raw_data.timestamp") >= col("summary_data.window.start")) &
            (col("raw_data.timestamp") <= col("summary_data.window.end"))))

    clean_data_with_stats = clean_data_with_stats.select(
        "raw_data.turbine_id", 
        "raw_data.timestamp", 
        "power_output", 
        "min_power_output", 
        "max_power_output", 
        "avg_power_output", 
        "stddev_power_output"
    )

    anomalies = clean_data_with_stats \
        .withColumn("deviation", (col("power_output") - col("avg_power_output")) / col("stddev_power_output")) \
        .filter((col("deviation") > 2) | (col("deviation") < -2))

    return(anomalies)