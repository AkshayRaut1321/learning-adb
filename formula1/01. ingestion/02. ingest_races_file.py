# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1 - initialize

# COMMAND ----------

# MAGIC %run "../includes/initialization"

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /mnt/formula1dl10/raw/

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

race_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True)
                                  ])

# COMMAND ----------

race_df = spark.read.option("header", True).schema(race_schema).csv(f'{source_path}/races.csv')

# COMMAND ----------

race_df.show()

# COMMAND ----------

race_selected_df = race_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year") \
    .withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col, concat, lit, current_timestamp

race_combined_time_df = race_selected_df.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(' '), col("time")), 'yyyy-MM-dd HH:mm:ss'))

race_final_df = addIngestionDateColumn(race_combined_time_df)
    .select("race_id", "race_year", "round", "circuit_id", "name", "race_timestamp", "ingestion_date")

# COMMAND ----------

race_final_df.write.mode('overwrite').partitionBy('race_year').parquet(f'{destination_path}/races')

# COMMAND ----------

spark.read.parquet(f'{destination_path}/races').show()

# COMMAND ----------

spark.read.parquet(f'{destination_path}/races/race_year=1951').show()
