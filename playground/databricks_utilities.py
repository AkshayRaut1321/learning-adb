# Databricks notebook source
# MAGIC %fs
# MAGIC ls

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

for folder_name in dbutils.fs.ls('/'):
    print(folder_name)

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.notebook.run('./child-notebook', 10, {'input1': "Called from databricks_utilities", 'input2': 150})

# COMMAND ----------

# MAGIC %pip install pandas

# COMMAND ----------


