# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Cluster Scoped credentials
# MAGIC
# MAGIC ##### Steps to follow
# MAGIC 1. Set the spark config fs.azure.account.key in the cluster.
# MAGIC 2. List files from demo container.
# MAGIC 3. Read data from circuits.csv file.

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dlakshayraut.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlakshayraut.dfs.core.windows.net"))

# COMMAND ----------

circuitsDF = spark.read.csv("abfss://demo@formula1dlakshayraut.dfs.core.windows.net/circuits.csv")
display(circuitsDF)
