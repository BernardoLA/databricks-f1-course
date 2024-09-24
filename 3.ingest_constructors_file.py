# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest constructors json file 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.functions import col, lit

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

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
    .schema(constructors_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/constructors.json")
    

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

constructor_renamed_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("data_source", lit(v_data_source)) \
                                             .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# we keep saving as managed table
constructor_renamed_df.write.mode("overwrite").format("delta").saveAsTable("databricks_ws_2.f1_silver.constructor")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_ws_2.f1_silver.constructor
# MAGIC LIMIT 10;

# COMMAND ----------

dbutils.notebook.exit("Success")
