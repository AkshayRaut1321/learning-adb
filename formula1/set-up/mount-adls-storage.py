# Databricks notebook source
# MAGIC %run "../includes/initialization"

# COMMAND ----------

scope = "formula1-scope"
client_id = dbutils.secrets.get(scope = scope, key = "databricks-app-client-id")
tenant_id = dbutils.secrets.get(scope = scope, key = "databricks-app-tenant-id")
client_secret = dbutils.secrets.get(scope = scope, key = "databricks-app-client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{client_id}",
          "fs.azure.account.oauth2.client.secret": f"{client_secret}",
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_dls(container_name):
    dbutils.fs.mount(source = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/",
                    mount_point = f"/mnt/{storage_name}/{container_name}",
                    extra_configs = configs)


# COMMAND ----------

def unmount_dls(container_name):
    dbutils.fs.unmount(f"/mnt/{storage_name}/{container_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unmount raw container (if any)
# MAGIC dbutils.fs.unmount('/mnt/formula1dl10/raw')

# COMMAND ----------

mount_dls("raw")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check the mounted location
# MAGIC 
# MAGIC %fs
# MAGIC ls dbfs:/mnt/formula1dl10/

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unmount processed container (if any)
# MAGIC 
# MAGIC unmount_dls("raw")

# COMMAND ----------

# Mount processed container
mount_dls("processed")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls(f'/mnt/{storage_name}/raw'))

# COMMAND ----------

mount_dls("presentation")
