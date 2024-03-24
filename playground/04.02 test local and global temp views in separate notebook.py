# Databricks notebook source
# MAGIC %sql
# MAGIC select * From v_race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1)
# MAGIC FROM global_temp.gv_race_results
