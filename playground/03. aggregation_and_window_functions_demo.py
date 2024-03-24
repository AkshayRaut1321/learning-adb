# Databricks notebook source
# MAGIC %run "../formula1/includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate functions demo

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

race_results_2020 = race_results_df.filter("race_year=2020")

# COMMAND ----------

display(race_results_2020)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

display(race_results_2020.select(count("*")))

# COMMAND ----------

display(race_results_2020.select(count("race_name")))

# COMMAND ----------

display(race_results_2020.select(countDistinct("race_name")))

# COMMAND ----------

display(race_results_2020.select(sum("points")))

# COMMAND ----------

display(race_results_2020.filter("driver_name = 'Lewis Hamilton'").select(sum("points")))

# COMMAND ----------

display(race_results_2020.filter("driver_name = 'Lewis Hamilton'").select(sum("points"), countDistinct("race_name")))

# COMMAND ----------

display(race_results_2020.groupBy("driver_name").sum("points"))

# COMMAND ----------

display(race_results_2020.groupBy("driver_name").agg(sum("points"), countDistinct("race_name")))

# COMMAND ----------

race_results_2019_2020 = race_results_df.filter("race_year in (2019, 2020)")

# COMMAND ----------

display(race_results_2019_2020)

# COMMAND ----------

races_grouped_df = race_results_2019_2020.groupBy("race_year", "driver_name").agg(sum("points"), countDistinct("race_name")) \
    .withColumnRenamed("sum(points)", "total_points") \
    .withColumnRenamed("count(race_name)", "total_unique_races");

display(races_grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))

display(races_grouped_df.withColumn("rank", rank().over(driverRankSpec)));
