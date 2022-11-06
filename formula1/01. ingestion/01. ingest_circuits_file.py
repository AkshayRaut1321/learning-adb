# Databricks notebook source
# MAGIC %fs
# MAGIC ls dbfs:/mnt/formula1dl10/raw/

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
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

#### one way is to infer schema
# circuits_df = spark.read \
#    .option("header", True) \
#    .option("inferSchema", True) \
#    .csv('dbfs:/mnt/formula1dl10/raw/circuits.csv')
#### another way is to pass your schema to the read function as follows:
circuits_df = spark.read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv('dbfs:/mnt/formula1dl10/raw/circuits.csv')

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# In the above output, you may notice that this will still print the the Nullable as true for circuitsId that's because of the standard behavior of DataFrame reader.
# In order to handle nulls, it needs to be handled explicitly.

# COMMAND ----------

# circuits_df.show()
display(circuits_df)

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

#### Test selecting specific columns
circuits_df.select("*", "circuitsId").show()

# COMMAND ----------

circuits_df.select("circuitsId", "circuitRef", "name", "location", "country", "lat", "lng", "alt").show()

# COMMAND ----------

circuits_df.select(circuits_df.circuitsId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt).show()

# COMMAND ----------

circuits_df.select(circuits_df["circuitsId"], circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"], circuits_df["country"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"]).show()

# COMMAND ----------

from pyspark.sql.functions import col

circuits_df.select(col("circuitsId").alias("circuits_id"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt")).show()
