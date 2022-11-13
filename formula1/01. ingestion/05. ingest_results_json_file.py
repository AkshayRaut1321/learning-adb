# Databricks notebook source
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

results_df = spark.read.schema(results_schema).json('/mnt/formula1dl10/raw/results.json')

# COMMAND ----------

results_concise_df = results_df.drop('statusId')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

results_renamed_df = results_concise_df.withColumnRenamed('resultId', 'result_id') \
        .withColumnRenamed('raceId', 'race_id') \
        .withColumnRenamed('driverId', 'driver_id') \
        .withColumnRenamed('constructorId', 'constructor_id') \
        .withColumnRenamed('positionText', 'position_text') \
        .withColumnRenamed('positionOrder', 'position_order') \
        .withColumnRenamed('fastestLap', 'fastest_lap') \
        .withColumnRenamed('fastestLapTime', 'fastest_lap_time') \
        .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed') \
        .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

results_renamed_df.write.mode('overwrite').partitionBy('race_id').parquet('/mnt/formula1dl10/processed/results')

# COMMAND ----------

spark.read.parquet('/mnt/formula1dl10/processed/results/race_id=81').show()
