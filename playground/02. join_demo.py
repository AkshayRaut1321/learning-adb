# Databricks notebook source
# MAGIC %run "../formula1/includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f'{destination_path}/races').filter("race_year = 2020") \
    .withColumnRenamed('name', 'race_name')

# COMMAND ----------

circuits_df = spark.read.parquet(f'{destination_path}/circuits') \
    .withColumnRenamed('name', 'circuit_name')

# COMMAND ----------

races_and_circuits_df = circuits_df.join(races_df, races_df.circuit_id == circuits_df.circuits_id) \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

races_and_circuits_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Left join

# COMMAND ----------

races_df = spark.read.parquet(f'{destination_path}/races').filter("race_year = 2020") \
    .withColumnRenamed('name', 'race_name')

# COMMAND ----------

circuits_df = spark.read.parquet(f'{destination_path}/circuits') \
    .filter("circuits_id < 70") \
    .withColumnRenamed('name', 'circuit_name')

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Circuits which don't have races

# COMMAND ----------

circuits_missing_races_df = circuits_df.join(races_df, races_df.circuit_id == circuits_df.circuits_id, "left") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Races which don't have circuits

# COMMAND ----------

races_missing_circuits_df = circuits_df.join(races_df, races_df.circuit_id == circuits_df.circuits_id, "right") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(races_missing_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Full outer join

# COMMAND ----------

races_and_circuits_missing_each_other_df = circuits_df.join(races_df, races_df.circuit_id == circuits_df.circuits_id, "full") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(races_and_circuits_missing_each_other_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Semi/Left semi join

# COMMAND ----------

circuits_semi_df = circuits_df.join(races_df, races_df.circuit_id == circuits_df.circuits_id, "semi")

# COMMAND ----------

display(circuits_semi_df)

# COMMAND ----------

races_semi_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuits_id, "semi")

# COMMAND ----------

display(races_semi_df)

# COMMAND ----------

display(races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuits_id, "anti"))

# COMMAND ----------

display(races_df.crossJoin(circuits_df))

# COMMAND ----------

display(races_df.count() * circuits_df.count())
