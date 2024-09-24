# Databricks notebook source
# MAGIC %md
# MAGIC #####Step1 - read the Json file
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.functions import current_timestamp, lit
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

pit_stops_schema = StructType(fields = [
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
])

# COMMAND ----------

df_mult_json = spark.read \
    .schema(pit_stops_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json", multiLine=True)

# COMMAND ----------

df_mult_json_renamed = df_mult_json \
    .withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("driverId","driver_id") \
    .withColumn("ingestion_date",current_timestamp()) \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# results will be an incremental load. therefore we update the records that could come in new batches and add unexisting ones in the 
# by includin the race_id in the merge condition and help spark to find the keys and avoid looping over all partition for each result id.
merge_condition = "tgt.race_id = upd.race_id AND tgt.driver_id = upd.driver_id AND tgt.stop = upd.stop"
merge_delta_data(input_df = df_mult_json_renamed, \
                 db_name = "f1_silver", \
                 table_name = "pit_stops", \
                 merge_condition = merge_condition, \
                 partition_column = "race_id", \
                 catalog_name="databricks_ws_2")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, driver_id, COUNT(1)
# MAGIC FROM databricks_ws_2.f1_silver.pit_stops
# MAGIC GROUP BY race_id, driver_id
# MAGIC LIMIT 10;

# COMMAND ----------


