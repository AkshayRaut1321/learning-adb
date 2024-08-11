# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS tokens
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container.
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlakshayraut.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dlakshayraut.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dlakshayraut.dfs.core.windows.net", "sv=2022-11-02&ss=b&srt=co&sp=rwlx&se=2024-08-12T00:39:14Z&st=2024-08-11T16:39:14Z&spr=https&sig=ZeTUmQbT5%2BXc5aOnFK15bKLxCA4yOL1R4IKFyn%2BhnAA%3D")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dlakshayraut.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlakshayraut.dfs.core.windows.net"))

# COMMAND ----------

circuitsDF = spark.read.csv("abfss://demo@formula1dlakshayraut.dfs.core.windows.net/circuits.csv")
display(circuitsDF)

# COMMAND ----------

