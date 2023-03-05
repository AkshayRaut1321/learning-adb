# Databricks notebook source
# MAGIC %run "../formula1/includes/initialization"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_path}/race_results");

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating temp SQL view from DataFrame

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results");

# COMMAND ----------

# MAGIC %md
# MAGIC ### Accessing SQL view using SQL cell

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from v_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

# MAGIC %md
# MAGIC ### Accessing SQL view from Python cell

# COMMAND ----------

p_race_year = 2019;

# COMMAND ----------

race_2019_df = spark.sql(f"SELECT COUNT(1) FROM v_race_results WHERE race_year = {p_race_year}");
display(race_2019_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Global temp view from DataFrame

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results");

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Spark will register the global temp view against a database called global_temp

# COMMAND ----------

# MAGIC %md
# MAGIC ##### To see local temp views

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC ##### to see global temp views

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1)
# MAGIC FROM global_temp.gv_race_results
