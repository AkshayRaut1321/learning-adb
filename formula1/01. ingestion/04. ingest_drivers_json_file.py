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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

drivers_schema = StructType(fields = [StructField("driverId", IntegerType(), False),
                                     StructField("driverRef", StringType(), True),
                                     StructField("number", IntegerType(), True),
                                     StructField("code", StringType(), True),
                                     StructField("name", StructType(fields=[StructField("forename", StringType(), False),
                                                                           StructField("surname", StringType(), False)]), True),
                                     StructField("dob", DateType(), True),
                                     StructField("nationality", StringType(), True),
                                     StructField("url", StringType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Load drivers' JSON data using drivers_schema

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f'{source_path}/drivers.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - drop the url column

# COMMAND ----------

drivers_concise_df = drivers_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Combine name columns into one
# MAGIC ###### Either you can overwrite the name data on the existing 'name' column as follows:
# MAGIC ##### OR
# MAGIC ###### You can create a new column for name with a different column name, e.g. 'full_name' and then drop the existing 'name'

# COMMAND ----------

# We are going ahead with replacing existing column.
from pyspark.sql.functions import concat, col, lit

drivers_renamed_df = drivers_concise_df.withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname'))) \
                                    .withColumnRenamed('driverId', 'driver_id') \
                                    .withColumnRenamed('driverRef', 'driver_ref') \
                                    .withColumn('data_source', lit(v_data_source))

drivers_final_df = addIngestionDateColumn(drivers_renamed_df, 'ingestion_date'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write drivers' JSON data to parquet

# COMMAND ----------

drivers_final_df.write.mode('overwrite').parquet(f'{destination_path}/drivers')

# COMMAND ----------

spark.read.parquet(f'{destination_path}/drivers').show()

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
