# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Service Principal
# MAGIC
# MAGIC ##### Steps to follow
# MAGIC 1. Register Azure AD application or service principal
# MAGIC 2. Generate a secret/password for the application.
# MAGIC 3. Set spark config with app/client id, directory/tenant id, and secret.
# MAGIC 4. Assign role 'Storage Blob Data Contributor' to the Data Lake.

# COMMAND ----------

client_id = "77af3c18-4fc5-403f-9698-0f009371a2cb";
tenant_id = "516ca49d-315a-440c-b7b0-0198f5b12503";
client_secret = "PBA8Q~KlRVvwE_8JPsKeWJONTgET~jX78ozVPbRU";


# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlakshayraut.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlakshayraut.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlakshayraut.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlakshayraut.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlakshayraut.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dlakshayraut.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlakshayraut.dfs.core.windows.net"))

# COMMAND ----------

circuitsDF = spark.read.csv("abfss://demo@formula1dlakshayraut.dfs.core.windows.net/circuits.csv")
display(circuitsDF)

# COMMAND ----------

