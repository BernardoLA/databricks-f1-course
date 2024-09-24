# Databricks notebook source
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

lap_times_schema = StructType(fields = [
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
])

# COMMAND ----------

df_lap_times = spark.read \
    .schema(lap_times_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

lap_times_with_ingestion_date_df = add_ingestion_date(df_lap_times)

# COMMAND ----------

final_df = lap_times_with_ingestion_date_df \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

merge_condition = "tgt.race_id = upd.race_id AND tgt.driver_id = upd.driver_id AND tgt.lap = upd.lap"
merge_delta_data(final_df,"f1_processed","lap_times", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.f1_processed.lap_times
