# Databricks notebook source
# MAGIC %run "../includes/initialization"

# COMMAND ----------

races_df = spark.read.parquet(f'{presentation_path}/race_results');
display(races_df)

# COMMAND ----------

from pyspark.sql.functions import sum, count, when

races_wins_grouped_df = races_df.groupBy('race_year', 'driver_name', 'driver_nationality', 'team') \
    .agg(sum('points').alias('total_points'), 
        count(when(races_df.position == 1, True)).alias('wins'));

display(races_wins_grouped_df.filter('race_year = 2020'))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank

driverRankByWins = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'));

driverStandingsDf = races_wins_grouped_df.withColumn('rank', rank().over(driverRankByWins));

display(driverStandingsDf);

# COMMAND ----------

driverStandingsDf.write.mode('overwrite').parquet(f"{presentation_path}/driver_standings")
