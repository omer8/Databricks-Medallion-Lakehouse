# Databricks notebook source
# List the folders we want to process today.
datasets_to_process = ["bookings", "flights", "passengers", "airports"]

# Push this list to the Databricks Job so the next task can pick it up 
dbutils.jobs.taskValues.set(
    key="table_list",
    value=datasets_to_process
)

print(f"Passed these datasets to the job: {datasets_to_process}")