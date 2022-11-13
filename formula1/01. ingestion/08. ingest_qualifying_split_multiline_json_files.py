# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1 - initialize

# COMMAND ----------

# MAGIC %run "../includes/initialization"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

qualifying_schema = StructType(fields = [StructField('qualifyId', IntegerType(), False),
                                      StructField('raceId', IntegerType(), True),
                                      StructField('driverId', IntegerType(), True),
                                      StructField('constructorId', IntegerType(), True),
                                      StructField('number', IntegerType(), True),
                                      StructField('position', IntegerType(), True),
                                      StructField('q1', StringType(), True),
                                      StructField('q2', StringType(), True),
                                      StructField('q3', StringType(), True)])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema) \
    .option('multiLine', True) \
    .json(source_path + '/qualifying/qualifying*.json')

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed('qualifyId', 'qualify_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('constructorId', 'constructor_id')

addIngestionDateColumn(qualifying_final_df) \
    .write.mode('overwrite') \
    .parquet(destination_path + '/qualifying')

# COMMAND ----------

spark.read.parquet(destination_path + '/qualifying').show()
