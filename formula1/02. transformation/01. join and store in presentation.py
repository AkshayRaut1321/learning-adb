# Databricks notebook source
# MAGIC %run "../includes/initialization"

# COMMAND ----------

races_df = spark.read.parquet(f'{destination_path}/races') \
    .withColumnRenamed('name', 'race_name') \
    .withColumnRenamed('race_timestamp', 'race_date') \
    .select('race_id', 'circuit_id', 'race_name', 'race_date', 'race_year')

# COMMAND ----------

circuits_df = spark.read.parquet(f'{destination_path}/circuits') \
    .withColumnRenamed('name', 'circuit_name') \
    .withColumnRenamed('location', 'circuit_location') \
    .select('circuits_id', 'circuit_location')

# COMMAND ----------

constructors_df = spark.read.parquet(f'{destination_path}/constructors') \
    .withColumnRenamed('name', 'team') \
    .select('constructor_id', 'team')

# COMMAND ----------

drivers_df = spark.read.parquet(f'{destination_path}/drivers') \
    .withColumnRenamed('name', 'driver_name') \
    .withColumnRenamed('number', 'driver_number') \
    .withColumnRenamed('nationality', 'driver_nationality') \
    .select('driver_id', 'driver_name', 'driver_number', 'driver_nationality')

# COMMAND ----------

results_df = spark.read.parquet(f'{destination_path}/results') \
    .withColumnRenamed('fastest_lap', 'fastest lap') \
    .withColumnRenamed('time', 'race time') \
    .select('driver_id', 'constructor_id', 'race_id', 'grid', 'fastest lap', 'race time', 'points', 'position')

# COMMAND ----------



final_df = results_df.join(drivers_df, drivers_df.driver_id == results_df.driver_id, 'inner') \
    .join(constructors_df, constructors_df.constructor_id == results_df.constructor_id, 'inner') \
    .join(races_df, races_df.race_id == results_df.race_id, 'inner') \
    .join(circuits_df, circuits_df.circuits_id == races_df.circuit_id, 'inner') \
    .select('race_year', 'race_name', 'race_date', 'circuit_location', 'driver_name', 'driver_number', 'driver_nationality', 'team', 'grid', 'fastest lap', 'race time', 'points', 'position')

final_df = addIngestionDateColumn(final_df, 'creation_date')

# COMMAND ----------

#display(final_df.filter("race_year = 2020 and race_name = 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

final_df.write.mode('overwrite').parquet(f'{presentation_path}/race_results')

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /mnt/formula1dlakshayraut/presentation/
