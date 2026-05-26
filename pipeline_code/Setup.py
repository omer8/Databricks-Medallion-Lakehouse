# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS flights_catalog
# MAGIC     MANAGED LOCATION 's3://databricks-flights-project/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS flights_catalog.raw

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS flights_catalog.raw.raw_flights_vol
# MAGIC     LOCATION 's3://databricks-flights-project/raw_data/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS flights_catalog.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS flights_catalog.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS flights_catalog.gold

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS flights_catalog.bronze.pipeline_state;