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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

pit_stops_schema = StructType(fields = [StructField('raceId', IntegerType(), True),
                                        StructField('driverId', IntegerType(), True),
                                        StructField('stop', StringType(), True),
                                        StructField('lap', IntegerType(), True),
                                        StructField('time', StringType(), True),
                                        StructField('duration', StringType(), True),
                                        StructField('milliseconds', IntegerType(), True)
                                       ]
                             )

# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema) \
    .option('multiLine', True) \
    .json(f'{source_path}/pit_stops.json')

# COMMAND ----------

from pyspark.sql.functions import lit

pit_stops_renamed_df = pit_stops_df.withColumnRenamed('raceId', 'race_id') \
            .withColumnRenamed('driverId', 'driver_id') \
            .withColumn('data_source', lit(v_data_source))

pit_stops_final_df = addIngestionDateColumn(pit_stops_renamed_df)

# COMMAND ----------

pit_stops_final_df.write.mode('overwrite').parquet(f'{destination_path}/pit_stops')

# COMMAND ----------

spark.read.parquet(f'{destination_path}/pit_stops').show()

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
