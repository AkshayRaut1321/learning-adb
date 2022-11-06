# Databricks notebook source
# MAGIC %fs
# MAGIC 
# MAGIC ls /mnt/formula1dl10/raw/

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

race_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", StringType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True)
                                  ])

# COMMAND ----------

race_df = spark.read.option("header", True).schema(race_schema).csv('/mnt/formula1dl10/raw/races.csv')

# COMMAND ----------

race_df.show()

# COMMAND ----------

race_selected_df = race_df.select("raceId", "year", "round", "circuitId", "name", "date", "time") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year") \
    .withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col, concat, lit, current_timestamp

race_final_df = race_selected_df.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(' '), col("time")), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("ingestion_date", current_timestamp()) \
    .select("race_id", "race_year", "round", "circuit_id", "name", "race_timestamp", "ingestion_date")

# COMMAND ----------

race_final_df.write.mode('overwrite').parquet('/mnt/formula1dl10/processed/races')

# COMMAND ----------

spark.read.parquet('/mnt/formula1dl10/processed/races').show()
