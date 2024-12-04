# Databricks notebook source
dbutils.widgets.text('can_start', "false")
v_can_start = dbutils.widgets.get('can_start')

if v_can_start == "false":
    dbutils.notebook.exit('can_start was False')

# COMMAND ----------

v_result = dbutils.notebook.run('01. join and store in presentation', 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

if v_result == "SUCCESS":
    v_result = dbutils.notebook.run('02. driver_standings_by_position', 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

if v_result == "SUCCESS":
    v_result = dbutils.notebook.run('03. constructor_or_team_standings', 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

dbutils.notebook.exit("All transformation completed")
