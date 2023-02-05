# Databricks notebook source
# MAGIC %md
# MAGIC ### 1 - Import the file into a DataFrame

# COMMAND ----------

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
# MAGIC #### Step 1 - Import data types for creating a schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Create a schema for your requirement by using imported types

# COMMAND ----------

circuits_schema = StructType(fields = [StructField("circuitsId", IntegerType(), False),
                                      StructField("circuitRef", StringType(), True),
                                      StructField("name", StringType(), True),
                                      StructField("location", StringType(), True),
                                      StructField("country", StringType(), True),
                                      StructField("lat", DoubleType(), True),
                                      StructField("lng", DoubleType(), True),
                                      StructField("alt", IntegerType(), True),
                                      StructField("url", StringType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Read the CSV file by specifying header option and your custom schema

# COMMAND ----------

#### one way is to infer schema
# circuits_df = spark.read \
#    .option("header", True) \
#    .option("inferSchema", True) \
#    .csv('dbfs:/mnt/formula1dl10/raw/circuits.csv')
#### another way is to pass your schema to the read function as follows:
circuits_df = spark.read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv('dbfs:'f'{source_path}/circuits.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Verify the data type of your DataFrame object

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6 - Verify the schema of your DataFrame object.

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# In the above output, you may notice that this will still print the the Nullable as true for circuitsId that's because of the standard behavior of DataFrame reader.
# In order to handle nulls, it needs to be handled explicitly.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 7 - Display the data of your DataFrame object.

# COMMAND ----------

# circuits_df.show()
display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 8 - Get the summary of your data.

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5 different ways of reading column specific data.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5.1) reading using literal string columns.
# MAGIC 
# MAGIC ##### Test selecting specific columns
# MAGIC E.g. 1) circuits_df.select("*", "circuitsId").show()
# MAGIC 
# MAGIC E.g. 2) circuits_df.select("circuitsId", "circuitRef", "name", "location", "country", "lat", "lng", "alt").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5.2) reading using each column as a property of the DataFrame object.
# MAGIC 
# MAGIC circuits_df.select(circuits_df.circuitsId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5.3) reading using each column as a key value property of the DataFrame object.
# MAGIC 
# MAGIC circuits_df.select(circuits_df["circuitsId"], circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"], circuits_df["country"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5.4) reading by importing and using col function that gives us a rename/alias functionality.
# MAGIC 
# MAGIC from pyspark.sql.functions import col
# MAGIC 
# MAGIC circuits_df.select(col("circuitsId").alias("circuits_id"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt")).show()

# COMMAND ----------

# - Method 4:
from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col("circuitsId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 5.5) reading by using withColumnRenamed function of the DataFrame object.

# COMMAND ----------

### Method 5:
from pyspark.sql.functions import lit

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitsId", "circuits_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("name", "name") \
    .withColumnRenamed("location", "location") \
    .withColumnRenamed("country", "country") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude") \
    .withColumn('data_source', lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 9 - Verify that columns are renamed in the output.

# COMMAND ----------

circuits_renamed_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 10 - Add new column using DataFrame.withColumn(<column_name>, column_object) function

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 10.1) - Add new column by passing a readymade function that returns a column object with.
# MAGIC 
# MAGIC from pyspark.sql.functions import current_timestamp
# MAGIC 
# MAGIC circuits_renamed_df.withColumn("ingestion_date", current_timestamp()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 10.2) - Add new column by passing a literal value to a function that returns a column object.
# MAGIC 
# MAGIC from pyspark.sql.functions import lit
# MAGIC 
# MAGIC circuits_selected_df.withColumn("env", lit("Development")).show()

# COMMAND ----------

# We will be creating a column using current_timestamp() function:
from pyspark.sql.functions import current_timestamp

circuits_final_df = addIngestionDateColumn(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 2 - Writing to a Parquet file using DataFrame Writer API

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f'{destination_path}/circuits')

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /mnt/formula1dlakshayraut/processed/

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3 - Reading from a Parquet file

# COMMAND ----------

parquet_df = spark.read.parquet(f'{destination_path}/circuits')

# COMMAND ----------

display(parquet_df)

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
