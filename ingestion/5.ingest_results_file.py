# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.functions import lit, col
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

## Create schema 
results_schema = StructType(fields = [
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", IntegerType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", FloatType(), True),
    StructField("statusId", IntegerType(), True),
])

# COMMAND ----------

## read file
results_df = spark.read \
    .schema(results_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# add two columns with data source and file date and rename others
results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# add ingestion date column
results_with_ingestion_date_df = add_ingestion_date(results_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Method 2

# COMMAND ----------

# we need to add the race_id (the partition) to our last column to be able to use the insertInto method
results_final_df = results_with_ingestion_date_df.select(
    "result_id","driver_id","constructor_id","number","grid","position","position_text","position_order","points","time","milliseconds","rank","fastest_lap_time","fastest_lap_speed","statusId","data_source","file_date","ingestion_date","race_id"
)

# COMMAND ----------

# there was duplicate records in the results table, we need to deduplicate the data
results_deduped_df = results_final_df.dropDuplicates(["race_id","driver_id"])

# COMMAND ----------

# results will be an incremental load. therefore we update the records that could come in new batches and add unexisting ones in the 
# by includin the race_id in the merge condition and help spark to find the keys and avoid looping over all partition for each result id.
merge_condition = "tgt.result_id = upd.result_id AND tgt.race_id = upd.race_id"
merge_delta_data(input_df = results_deduped_df, \
                 db_name = "f1_silver", \
                 table_name = "results", \
                 merge_condition = merge_condition, \
                 partition_column = "race_id", \
                 catalog_name="databricks_ws_2")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM databricks_ws_2.f1_silver.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- there are some duplicates in the data that we need to take care of 
# MAGIC -- we'll just use dropDuplicates function and let Spark decide which record to pick
# MAGIC SELECT race_id, driver_id, COUNT(1)
# MAGIC   FROM databricks_ws_2.f1_silver.results
# MAGIC   GROUP BY race_id, driver_id
# MAGIC   HAVING COUNT(1) > 1;

# COMMAND ----------


