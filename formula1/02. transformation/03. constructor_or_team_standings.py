# Databricks notebook source
# MAGIC %run "../includes/initialization"

# COMMAND ----------

races_df = spark.read.parquet(f'{presentation_path}/race_results');
display(races_df)

# COMMAND ----------

from pyspark.sql.functions import sum, count, when

team_wins_grouped_df = races_df.groupBy('race_year', 'team') \
    .agg(sum('points').alias('total_points'), 
        count(when(races_df.position == 1, True)).alias('wins'));

display(team_wins_grouped_df.filter('race_year = 2020'))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

teamRankByWins = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'));

teamStandingsDf = team_wins_grouped_df.withColumn('rank', rank().over(teamRankByWins));

display(teamStandingsDf.filter("race_year = 2020"));

# COMMAND ----------

teamStandingsDf.write.mode('overwrite').parquet(f"{presentation_path}/constructor_standings")
