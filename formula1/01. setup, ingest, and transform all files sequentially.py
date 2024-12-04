# Databricks notebook source
dbutils.widgets.text('can_start', "false")
v_can_start = dbutils.widgets.get('can_start')

if v_can_start == "false":
    dbutils.notebook.exit('can_start was False')

# COMMAND ----------

v_result = dbutils.notebook.run('../set-up/mount-adls-storage', 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run('./01. ingestion/09. ingest_all_files_sequentially', 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

if v_result == "SUCCESS":
    v_result = dbutils.notebook.run('./02. transformation/04. transform_all_files_sequentially', 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

dbutils.notebook.exit("All transformation completed")
