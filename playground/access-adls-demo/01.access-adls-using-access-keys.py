# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Access Keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container.
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.key.formula1dlakshayraut.dfs.core.windows.net",
               "cWY7B3QEhpf1ObCULRmcWaiC8uoF3t3NGhlEnsx1jSVwl6hVQhNEtaglfn4jOMK7Mw2ynUGJQXsC+AStEDKnpg==")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dlakshayraut.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlakshayraut.dfs.core.windows.net"))

# COMMAND ----------

circuitsDF = spark.read.csv("abfss://demo@formula1dlakshayraut.dfs.core.windows.net/circuits.csv")
display(circuitsDF)

# COMMAND ----------

