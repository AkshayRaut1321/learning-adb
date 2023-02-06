# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1 - initialize

# COMMAND ----------

# MAGIC %run "../includes/initialization"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Take input parameters

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - create schema using Struct for root and nested objects

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

results_schema = StructType(fields = [StructField('resultId', IntegerType(), False),
                                     StructField('raceId', IntegerType(), False),
                                     StructField('driverId', IntegerType(), False),
                                     StructField('constructorId', IntegerType(), False),
                                     StructField('number', IntegerType(), False),
                                     StructField('grid', IntegerType(), False),
                                     StructField('position', IntegerType(), False),
                                     StructField('positionText', StringType(), False),
                                     StructField('positionOrder', IntegerType(), False),
                                     StructField('points', FloatType(), False),
                                     StructField('laps', IntegerType(), False),
                                     StructField('time', StringType(), False),
                                     StructField('milliseconds', IntegerType(), False),
                                     StructField('fastestLap', IntegerType(), False),
                                     StructField('rank', IntegerType(), False),
                                     StructField('fastestLapTime', StringType(), False),
                                     StructField('fastestLapSpeed', FloatType(), False),
                                     StructField('statusId', IntegerType(), False)])


# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f'{source_path}/results.json')

# COMMAND ----------

results_concise_df = results_df.drop('statusId')

# COMMAND ----------

from pyspark.sql.functions import lit

results_renamed_df = results_concise_df.withColumnRenamed('resultId', 'result_id') \
        .withColumnRenamed('raceId', 'race_id') \
        .withColumnRenamed('driverId', 'driver_id') \
        .withColumnRenamed('constructorId', 'constructor_id') \
        .withColumnRenamed('positionText', 'position_text') \
        .withColumnRenamed('positionOrder', 'position_order') \
        .withColumnRenamed('fastestLap', 'fastest_lap') \
        .withColumnRenamed('fastestLapTime', 'fastest_lap_time') \
        .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed') \
        .withColumn('data_source', lit(v_data_source))

results_final = addIngestionDateColumn(results_renamed_df, 'ingestion_date'))

# COMMAND ----------

results_final.write.mode('overwrite').partitionBy('race_id').parquet(f'{destination_path}/results')

# COMMAND ----------

spark.read.parquet(f'{destination_path}/results/race_id=81').show()

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
