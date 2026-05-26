# Databricks notebook source
# MAGIC %sql
# MAGIC -- Give analysts the ability to see the catalog
# MAGIC GRANT USE CATALOG ON CATALOG flights_catalog TO `data_analysts`;
# MAGIC
# MAGIC -- Give analysts the ability to see the gold schema
# MAGIC GRANT USE SCHEMA ON SCHEMA flights_catalog.gold TO `data_analysts`;
# MAGIC
# MAGIC -- Give them permission to read all current tables in the gold schema
# MAGIC GRANT SELECT ON SCHEMA flights_catalog.gold TO `data_analysts`;
# MAGIC
# MAGIC -- EXPLICIT DENY: Ensure they cannot see the raw, bronze, or silver layers
# MAGIC REVOKE ALL PRIVILEGES ON SCHEMA flights_catalog.raw FROM `data_analysts`;
# MAGIC REVOKE ALL PRIVILEGES ON SCHEMA flights_catalog.bronze FROM `data_analysts`;
# MAGIC REVOKE ALL PRIVILEGES ON SCHEMA flights_catalog.silver FROM `data_analysts`;