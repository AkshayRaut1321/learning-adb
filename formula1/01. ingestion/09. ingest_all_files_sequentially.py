# Databricks notebook source
dbutils.widgets.text('p_can_start', "false")
v_can_start = dbutils.widgets.get('p_can_start')

if v_can_start == "false":
    dbutils.notebook.exit('can_start was False')

# COMMAND ----------

v_result = dbutils.notebook.run('01. ingest_circuits_file', 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

if v_result == "SUCCESS":
    v_result = dbutils.notebook.run('02. ingest_races_file', 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

if v_result == "SUCCESS":
    v_result = dbutils.notebook.run('03. ingest_constructors_json_file', 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

if v_result == "SUCCESS":
    v_result = dbutils.notebook.run('04. ingest_drivers_json_file', 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

if v_result == "SUCCESS":
    v_result = dbutils.notebook.run('05. ingest_results_json_file', 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

if v_result == "SUCCESS":
    v_result = dbutils.notebook.run('06. ingest_pit_stops_multiline_json_file', 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

if v_result == "SUCCESS":
    v_result = dbutils.notebook.run('07. ingest_lap_times_split_csv_files', 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

if v_result == "SUCCESS":
    v_result = dbutils.notebook.run('08. ingest_qualifying_split_multiline_json_files', 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

dbutils.notebook.exit("All ingestion completed")
