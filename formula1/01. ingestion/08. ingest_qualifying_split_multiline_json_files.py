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

from pyspark.sql.functions import lit

qualifying_final_df = qualifying_df.withColumnRenamed('qualifyId', 'qualify_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('constructorId', 'constructor_id') \
    .withColumn('data_source', lit(v_data_source))

addIngestionDateColumn(qualifying_final_df, 'ingestion_date') \
    .write.mode('overwrite') \
    .parquet(destination_path + '/qualifying')

# COMMAND ----------

spark.read.parquet(destination_path + '/qualifying').show()

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
