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

# MAGIC %md
# MAGIC Similar to results table we have to re-process the data for updated records in new batches. However there is one difference:
# MAGIC - we do an aggregation at the race year level to calculate the driver standings.
# MAGIC - therefore we need to bring the all the data existing for the race years up to the point of a batch.
# MAGIC - otherwise, we'd miss the races data from old batches when doing the aggregation. 
# MAGIC - then we first get a list of unique race years we have in the batch.
# MAGIC - after that we read the data from silver race_results again and filter for the unique years in the race years list.
# MAGIC - then we can calculate the driver standings.

# COMMAND ----------

# we adjust this code to be incremental.
# we need to loop over each file as others, but here driver standings is at year level.
# and each file can have data from the same year, like 2021-03-28 and 2021-04-18, in which case we need to account for that.
race_results_df = spark.read.format("delta") \
    .load(f"{presentation_folder_path}/race_results") \
    .filter(f"file_date = '{v_file_date}'")


# COMMAND ----------

# so for each file we take a list of unique years with function below:
race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

# then we read again the race_results file and filter only for years we have in the list.
# then we calculate the drivers standings below and do the merge as we did for the other files.
race_results_df = spark.read.format("delta") \
    .load(f"{presentation_folder_path}/race_results") \
    .filter(col("race_year") \
    .isin(race_year_list))

# COMMAND ----------

# we calculate both total points for a driver in a given year and how many wins a driver has in a given year
driver_standing_df = race_results_df \
    .groupBy("race_year","driver_name","driver_nationality") \
    .agg(sum("points").alias("total_points"), \
        count(when(col("position")==1, True)).alias("wins")
        )

# COMMAND ----------

rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standing_df.withColumn("rank", rank().over(rank_spec))

# COMMAND ----------

# here we are using race_year as partition column because our data is aggregated a race_year therefore it'd make more sense to do so
merge_condition = "tgt.driver_name = upd.driver_name AND tgt.race_year = upd.race_year"
merge_delta_data(input_df=final_df,
                 db_name="f1_presentation", 
                 table_name="driver_standings", 
                 merge_condition=merge_condition,
                 partition_column="race_year",
                 catalog_name="databricks_ws_2")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.f1_presentation.driver_standings WHERE race_year = 2021
