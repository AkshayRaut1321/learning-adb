# Databricks notebook source
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
    .json('/mnt/formula1dl10/raw/pit_stops.json')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

pit_stops_final_df = pit_stops_df.withColumnRenamed('raceId', 'race_id') \
            .withColumnRenamed('driverId', 'driver_id') \
            .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

pit_stops_final_df.write.mode('overwrite').parquet('/mnt/formula1dl10/processed/pit_stops')

# COMMAND ----------

spark.read.parquet('/mnt/formula1dl10/processed/pit_stops').show()
