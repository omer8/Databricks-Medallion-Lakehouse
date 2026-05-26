# Databricks notebook source
import pyspark.sql.functions as F

# Set up a parameter.
dbutils.widgets.text("table_name", "bookings") 

# Grab the specific dataset name passed by the orchestrator
dataset = dbutils.widgets.get("table_name")
print(f"Starting work on: {dataset}")

# Where the raw data comes from
raw_volume = "/Volumes/flights_catalog/raw/raw_flights_vol"
source_path = f"{raw_volume}/{dataset}/"

# Where the pipeline memory goes to (Bronze Managed Volume)
state_volume = "/Volumes/flights_catalog/bronze/pipeline_state"
schema_path = f"{state_volume}/_schemas/{dataset}"          
checkpoint_path = f"{state_volume}/_checkpoints/{dataset}"  

# Where the final table lives
target_table = f"flights_catalog.bronze.{dataset}"          


# --- INGESTION ---
# Read the new CSV files using Auto Loader
df_raw = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", schema_path)
  .option("header", "true")                         
  .load(source_path)
)

# Adds the exact date and time the row was ingested
df_enriched = (df_raw.withColumn("last_updated", F.current_timestamp())   
                     .withColumn("source_file", F.expr("_metadata.file_path"))    
)

# --- STORAGE ---
# Append the new data to our Bronze table
(df_enriched.writeStream
  .format("delta")
  .outputMode("append")                             
  .option("checkpointLocation", checkpoint_path)    
  .option("mergeSchema", "true")                    
  .trigger(availableNow=True)                       
  .toTable(target_table)
)

print(f"All done loading data into {target_table}")

# COMMAND ----------

