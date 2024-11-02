# Databricks notebook source
# MAGIC %md 
# MAGIC ### Create the Star Schema

# COMMAND ----------

# import functions and delta tables
from pyspark.sql.functions import current_timestamp
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
# MAGIC #####Load tables:
# MAGIC 1. Dimension Tables - Drivers, constructors, circuits, and races full load tables.
# MAGIC - Assumption: in each weekly batch the data should be replaced (they only get new data, not existing data won't be changed).
# MAGIC

# COMMAND ----------

# drivers dimensional full load table
drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality") \
.withColumnRenamed("id", "driver_id")

# COMMAND ----------

# constructors dimensional full load table
constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructor") \
.withColumnRenamed("name", "team") 

# COMMAND ----------

# circuits dimensional full load table
circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits") \
.withColumnRenamed("location", "circuit_location") \
.withColumnRenamed("id", "circuit_id")

# COMMAND ----------

# races dimensional full load table
races_df = spark.read.format("delta").load(f"{processed_folder_path}/races") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_time_stamp", "race_date") 

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Fact Table - Results incremental load table.
# MAGIC
# MAGIC What happens in a nutshell:
# MAGIC -  records pushed to silver schema in 21-03-2021(1st batch) create gold tables (overwrite method).
# MAGIC -  records pushed to gold schema in 21-03-2021 (1st batch) create gold tables (overwrite method).
# MAGIC -  in 28-03-2021 new records are added together with existing records from 21-03-2021 with updated values
# MAGIC -  this leads to the upsert function to be triggered and update records in silver results table 
# MAGIC -  with updated values, these records also get new file_date (of giving batch where change happened). 
# MAGIC -  the updated records will then be pulled from the silver results table (together with new records from that batch).
# MAGIC -  and, finally, we repeat the upsert operation to update the existing records in gold and insert the new ones.
# MAGIC -  we don't need to reprocess data in old batches that are not changed in new batches.

# COMMAND ----------

# constructors facts incremental table - here we need a different approach
results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
.filter(f"file_date = '{v_file_date}'") \
.withColumnRenamed("time", "race_time") \
.withColumnRenamed("race_id", "result_race_id") \
.withColumnRenamed("file_date", "result_file_date") \
.withColumnRenamed("driver_id", "driver_id_results") 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join circuits to races

# COMMAND ----------

# We cannot join results directly on circuits. We first need join races to circuits.
race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
.select(races_df.race_id, \
    races_df.race_year, \
    races_df.race_name, \
    races_df.race_date, \
    circuits_df.circuit_location
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join results to all other dataframes

# COMMAND ----------

# Join the other tables on the respective keys
race_results_df = results_df.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id) \
                            .join(drivers_df, results_df.driver_id_results == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

# Select relevant columns, rename and create timestamp of ingestion date
final_df = race_results_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_id","driver_number", "driver_nationality",
                                 "team", "grid", "fastest_lap_time", "race_time", "points", "position", "result_file_date") \
                          .withColumn("created_date", current_timestamp()) \
                          .withColumnRenamed("result_file_date", "file_date") 

# COMMAND ----------

# we are using driver name becuase we dint
merge_condition = "tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id"
merge_delta_data(input_df=final_df,
                db_name='f1_gold',
                table_name='race_results',
                merge_condition=merge_condition,
                partition_column='race_id',
                catalog_name = "databricks_ws_2")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM databricks_ws_2.f1_gold.race_results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT file_date, race_year, race_id, COUNT(1)
# MAGIC FROM databricks_ws_2.f1_gold.race_results
# MAGIC WHERE (race_id = 1052 OR race_id =  1053)
# MAGIC GROUP BY file_date, race_year, race_id
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, race_year, COUNT(1)
# MAGIC FROM databricks_ws_2.f1_gold.race_results
# MAGIC WHERE race_year > 2019
# MAGIC GROUP BY race_id, race_year
