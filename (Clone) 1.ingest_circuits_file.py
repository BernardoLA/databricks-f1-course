# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest circuits.csv file

# COMMAND ----------

# check widgets attribute
# dbutils.widgets.help()

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

raw_folder_path
processed_folder_path

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the CSV File using the Spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, lit


# COMMAND ----------

## We are not going to infer schema because it execute two jobs (first reads data, then go over it to get schema.)
## Moreover, in case of errrors breaks the code. Therefore we will declare the schema with the types API 
#circuits_df = spark.read.csv("/mnt/formulaonedb/raw/circuits.csv", header=True, inferSchema=True)



# COMMAND ----------

## Declaring the Schema with API
# Structype stands for a row in a data frame
# StructField stands for each column in the row.
circuits_schema = StructType(fields=[StructField("circuitId",IntegerType(), False),
                                     StructField("circuitRef",StringType(), True),
                                     StructField("name",StringType(), True),
                                     StructField("location",StringType(), True),
                                     StructField("lat",DoubleType(), True),
                                     StructField("lng",DoubleType(), True),
                                     StructField("alt",IntegerType(), True),
                                     StructField("url",StringType(), True),                                    
                                    ])

# COMMAND ----------

## Get now the circuits data with the proper schema:
circuits_df = spark.read \
    .option("schema",circuits_schema) \
    .option("header", True) \
    .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####selecting/renaming/adding columns from data frame
# MAGIC

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId").alias("id"))

# COMMAND ----------

## Drop columns
circuits_drop_url = circuits_df.drop('url')
display(circuits_drop_url)

# COMMAND ----------

## Rename columns
circuits_renamed_df = circuits_df.withColumnRenamed("circuitId","id") \
    .withColumnRenamed("circuitRef","circuit_ref") \
    .withColumnRenamed("lat","latitude") \
    .withColumnRenamed("alt","altitude") \
    .withColumnRenamed("lng","longitude") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))


# COMMAND ----------

## add Ingestion Date to the data frame - current_timestamp returns and Column Object
circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write data to datalake as parquet

# COMMAND ----------

##Save data as parquet - mode overwrite to overwrite existing data if the case (other modes available in API)
# circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")
# circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")
# circuits_final_df.write.parquet(f"{processed_folder_path}/circuits", mode = "overwrite")
## check how file was saved in location

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("hive_metastore.f1_processed.circuits")
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("hive_metastore.f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM hive_metastore.f1_processed.circuits

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formulaonedb/processed/circuits

# COMMAND ----------

## query back this data
df = spark.read.format("delta").load(f"{processed_folder_path}/circuits")
display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
