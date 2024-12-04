# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using any mechanis,m by passing config directly to dbutil>mount function
# MAGIC
# MAGIC ##### Steps to follow
# MAGIC 1. Register Azure AD application or service principal
# MAGIC 2. Generate a secret/password for the application.
# MAGIC 3. Create a config with app/client id, directory/tenant id, and secret.
# MAGIC 4. Pass this config to dbutils.fs.mount function to mount the container.
# MAGIC 5. Assign role 'Storage Blob Data Contributor' to the Data Lake.

# COMMAND ----------

client_id = "4b229fbb-4f38-4728-9ab2-19b026e6ddbb";
tenant_id = "516ca49d-315a-440c-b7b0-0198f5b12503";
client_secret = "<client_secret>";

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{client_id}",
          "fs.azure.account.oauth2.client.secret": f"{client_secret}",
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(source = f"abfss://demo@formula1dlakshayraut.dfs.core.windows.net",
                    mount_point = f"/mnt/formula1dlakshayraut/demo",
                    extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls(f'/mnt/formula1dlakshayraut/demo'))

# COMMAND ----------

circuitsDF = spark.read.option("header", True).csv('dbfs:'f'/mnt/formula1dlakshayraut/raw/')
display(circuitsDF)

# COMMAND ----------

dbutils.fs.unmount(f"/mnt/formula1dlakshayraut/demo")
