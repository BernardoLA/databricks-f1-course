# Databricks notebook source
# MAGIC %md
# MAGIC #####Step 1 - Read the Json using the spark reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../../includes/configuration"

# COMMAND ----------

# MAGIC %run "../../includes/common_functions"

# COMMAND ----------

## with this json file we need to create the schema for the nested json object first
## and assign it to the broader schema
name_schema = StructType(fields=[StructField("forename",StringType(),True),
                                 StructField("surname", StringType(), True)
])

# COMMAND ----------

## we can now create the broader schema and include the name object
drivers_schema = StructType(fields = [StructField("driverId", IntegerType(), False),
                            StructField("driverRef", StringType(), True),
                            StructField("number", IntegerType(), True),
                            StructField("code", StringType(), True),
                            StructField("name", name_schema),
                            StructField("dob", DateType(), True),
                            StructField("nationality", StringType(), True),
                            StructField("url", StringType(), True)
])

# COMMAND ----------

## now read the json file from the raw container
drivers_df = spark.read \
    .schema(drivers_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

## We can now read our data to check 
# note: we cna sim that the name is in the expected json format
display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2 - add and remove columns

# COMMAND ----------

## now we can get the columns we need
drivers_with_columns = drivers_df \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3 - drop the unwanted columns
# MAGIC

# COMMAND ----------

drivers_final_df = drivers_with_columns.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 4 - write to processed container in paquet format

# COMMAND ----------

drivers_final_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("databricks_ws_2.f1_silver.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_ws_2.f1_silver.drivers
# MAGIC LIMIT 10;

# COMMAND ----------

dbutils.notebook.exit("Success")
