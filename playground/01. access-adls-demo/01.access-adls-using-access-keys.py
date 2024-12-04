# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Access Keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container.
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.key.formula1dlakshayraut.dfs.core.windows.net",
               "<storage-account-key>")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dlakshayraut.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlakshayraut.dfs.core.windows.net"))

# COMMAND ----------

circuitsDF = spark.read.csv("abfss://demo@formula1dlakshayraut.dfs.core.windows.net/circuits.csv")
display(circuitsDF)

# COMMAND ----------


