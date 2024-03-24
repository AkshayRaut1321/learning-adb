# Databricks notebook source
# MAGIC %run "../formula1/includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f'{destination_path}/races')

# COMMAND ----------

display(races_df.filter("race_year = 2020"))

# COMMAND ----------

display(races_df.filter(races_df.race_year == 2020))

# COMMAND ----------

display(races_df.filter(races_df["race_year"] == 2020))

# COMMAND ----------

display(races_df.filter((races_df["race_year"] == 2020) & (races_df["round"] < 5)))

# COMMAND ----------

display(races_df.where((races_df["race_year"] == 2020) & (races_df["round"] < 5)))
