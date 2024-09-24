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

display(df_mult_json)

# COMMAND ----------

df_mult_json_renamed = df_mult_json \
    .withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("driverId","driver_id") \
    .withColumn("ingestion_date",current_timestamp()) \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

merge_condition = "tgt.race_id = upd.race_id AND tgt.driver_id = upd.driver_id AND tgt.stop = upd.stop"
merge_delta_data(df_mult_json_renamed,"f1_processed","pit_stops", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.f1_processed.pit_stops;

# COMMAND ----------


