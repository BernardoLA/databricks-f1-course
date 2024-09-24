# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.functions import current_timestamp, lit
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

qualifying_schema = StructType(fields = [
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
])

# COMMAND ----------

df_qualifying= spark.read \
    .schema(qualifying_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/qualifying", multiLine=True)

# COMMAND ----------

qualifying_with_ingestion_date_df = add_ingestion_date(df_qualifying)

# COMMAND ----------

final_df = qualifying_with_ingestion_date_df \
    .withColumnRenamed("qualifyId", "qualify_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(final_df)

# COMMAND ----------

merge_condition = "tgt.qualify_id = upd.qualify_id AND tgt.race_id = upd.race_id"
merge_delta_data(final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT qualify_id, race_id, driver_id, position, ingestion_date, file_date -- Select a subset of useful columns
# MAGIC FROM hive_metastore.f1_processed.qualifying
# MAGIC WHERE race_id = 1 -- Filter on partition column to improve performance
# MAGIC   AND ingestion_date >= date_sub(current_date(), 30) -- Filter on date column to limit data to recent entries
# MAGIC LIMIT 100 -- Limit the number of rows returned
