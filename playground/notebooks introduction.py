# Databricks notebook source
# MAGIC %md
# MAGIC # This is introduction to Notebooks
# MAGIC We have learned about
# MAGIC * How to create notebooks
# MAGIC * UI
# MAGIC * Magic commands
# MAGIC * and documentation commands

# COMMAND ----------

msg = "Hello"

# COMMAND ----------

print(msg)

# COMMAND ----------

# MAGIC %sql SELECT 'hello' as Message

# COMMAND ----------

# MAGIC %scala
# MAGIC val msg2 = "new msg"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/COVID/USAFacts/

# COMMAND ----------

# MAGIC %fs
# MAGIC head 10 dbfs:/databricks-datasets/COVID/USAFacts/covid_confirmed_usafacts.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC ps

# COMMAND ----------


