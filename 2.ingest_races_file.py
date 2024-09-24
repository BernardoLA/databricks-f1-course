# Databricks notebook source
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DateType
from pyspark.sql.functions import current_timestamp, to_timestamp, lit, concat, col

# COMMAND ----------

dbutils.widgets.text("p_data_source", " ")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

## first define the schema
races_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                    StructField("year", IntegerType(), False),
                                    StructField("round", IntegerType(), False),
                                    StructField("circuitId", IntegerType(), False),
                                    StructField("name", StringType(), False),
                                    StructField("date", DateType(), True),
                                    StructField("time", StringType(), False),
                                    StructField("url", StringType(), False)
                                    ])

# COMMAND ----------

## now read the data
races_df = spark.read \
    .option("header", True) \
    .schema(races_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/races.csv")    


# COMMAND ----------

## add race timestamp and ingestion date of record
races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
    .withColumn("race_time_stamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss")) 

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

## select the columns we need
races_selected_df = races_with_timestamp_df.drop("date","time","url") 
races_selected_renamed_df = races_selected_df \
    .withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("year","race_year") \
    .withColumnRenamed("circuitId","circuit_id")

# COMMAND ----------

# making this table external by adding option with the path for learning purposes
# ipc: tried to use the mnt path but it did not work anymore. unity catalog does not accept it(?maybe addressed in the course)
races_selected_renamed_df.write.mode("overwrite").partitionBy("race_year").format("delta").option("path","abfss://processed@formulaonedb.dfs.core.windows.net/races_ext").saveAsTable("databricks_ws_2.f1_silver.races_ext")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Checking
# MAGIC SELECT *
# MAGIC FROM databricks_ws_2.f1_silver.races_ext
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formulaonedb/processed/races

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formulaonedb/processed/races_ext

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC TABLE EXTENDED databricks_ws_2.f1_silver.races_ext

# COMMAND ----------

##
display(spark.read.format("delta").load("/mnt/formulaonedb/processed/races_ext"))

# COMMAND ----------

dbutils.notebook.exit("Success!")
