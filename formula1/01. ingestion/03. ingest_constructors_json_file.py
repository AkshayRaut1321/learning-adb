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
# MAGIC ##### Step 1 - Load JSON file data in a DataFrame using a DDL schema instead of Structs.

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

constructors_df = spark.read.schema(constructors_schema).json(f'{source_path}/constructors.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop unwanted columns

# COMMAND ----------

constructors_concise_df = constructors_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns using snake case.

# COMMAND ----------

from pyspark.sql.functions import lit

constructors_renamed_df = constructors_concise_df.withColumnRenamed('constructorId', 'constructor_id') \
    .withColumnRenamed('constructorRef', 'constructor_ref') \
    .withColumn('data_source', lit(v_data_source))

constructors_final_df = addIngestionDateColumn(constructors_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Store JSON data at processed container in parquet form

# COMMAND ----------

constructors_final_df.write.mode('overwrite').parquet(f'{destination_path}/constructors')

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls mnt/formula1dl10/processed/constructors

# COMMAND ----------

spark.read.parquet(f'{destination_path}/constructors').show()

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
