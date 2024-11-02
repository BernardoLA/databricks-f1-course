# Databricks notebook source
from pyspark.sql.functions import col, count, when, sum, rank, desc
from pyspark.sql import Window
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# similar logic as for the drivers standings
# we have to aggregate at year level and different files can have data for multiple/same years.
# we need to account for that with that piece of code.
race_results_df = spark.read.format("delta") \
    .load(f"{presentation_folder_path}/race_results") \
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

race_results_df = spark.read.format("delta") \
    .load(f"{presentation_folder_path}/race_results") \
    .filter(col("race_year") \
    .isin(race_year_list))

# COMMAND ----------

constructor_standing_df = race_results_df \
    .groupBy("race_year","team") \
    .agg(sum("points").alias("total_points"), \
        count(when(col("position")==1, True)).alias("wins")
        )

# COMMAND ----------

display(constructor_standing_df.filter("race_year = 2020"))

# COMMAND ----------

rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = constructor_standing_df.withColumn("rank", rank().over(rank_spec))

# COMMAND ----------

merge_condition = "tgt.team = upd.team AND tgt.race_year = upd.race_year"
merge_delta_data(final_df, 'f1_presentation', 'constructor_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.f1_presentation.constructor_standings WHERE race_year = 2021;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, COUNT(1)
# MAGIC   FROM hive_metastore.f1_presentation.constructor_standings
# MAGIC  GROUP BY race_year
# MAGIC  ORDER BY race_year DESC;

# COMMAND ----------


