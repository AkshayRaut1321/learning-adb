# Databricks notebook source
dbutils.widgets.text('input1', '', "Enter text value")
dbutils.widgets.text('input2', '', "Enter number value")

# COMMAND ----------

input1 = dbutils.widgets.get('input1')
input2 = dbutils.widgets.get('input2')
print(input1)

# COMMAND ----------

dbutils.notebook.exit(input2)

# COMMAND ----------


