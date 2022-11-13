# Databricks notebook source
# MAGIC %md
# MAGIC ##### Step 1 - Load JSON file data in a DataFrame using a DDL schema instead of Structs.

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

constructors_df = spark.read.schema(constructors_schema).json('/mnt/formula1dl10/raw/constructors.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop unwanted columns

# COMMAND ----------

constructors_concise_df = constructors_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns using snake case.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

constructors_final_df = constructors_concise_df.withColumnRenamed('constructorId', 'constructor_id') \
    .withColumnRenamed('constructorRef', 'constructor_ref') \
    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Store JSON data at processed container in parquet form

# COMMAND ----------

constructors_final_df.write.mode('overwrite').parquet('/mnt/formula1dl10/processed/constructors')

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls mnt/formula1dl10/processed/constructors

# COMMAND ----------

spark.read.parquet('/mnt/formula1dl10/processed/constructors').show()
