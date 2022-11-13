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

lap_time_schema = StructType(fields = [StructField('race_id', IntegerType(), False),
                                      StructField('driver_id', IntegerType(), True),
                                      StructField('lap', IntegerType(), True),
                                      StructField('position', IntegerType(), True),
                                      StructField('time', StringType(), True),
                                      StructField('milliseconds', IntegerType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC #### There are two ways to read split files
# MAGIC ##### 1) Mention the path of the folder where files are kept.
# MAGIC spark.read.schema(lap_time_schema) \
# MAGIC     .csv('/mnt/formula1dl10/raw/lap_times').show()
# MAGIC ##### 2) Use wildcard pattern for matching file names, in case a folder contains different types of split files.
# MAGIC spark.read.schema(lap_time_schema) \
# MAGIC     .csv('/mnt/formula1dl10/raw/lap_times/lap_times*.csv').show()

# COMMAND ----------

lap_time_df = spark.read.schema(lap_time_schema) \
    .csv(f'{source_path}/lap_times/')

# COMMAND ----------

from pyspark.sql.functions import lit

addIngestionDateColumn(lap_time_df) \
    .withColumn('data_source', lit(v_data_source)) \
    .write.mode('overwrite') \
    .parquet(f'{destination_path}/lap_times')

# COMMAND ----------

spark.read.parquet(f'{destination_path}/lap_times').show()

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
