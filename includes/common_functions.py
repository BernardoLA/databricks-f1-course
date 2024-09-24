# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

# Read the df from the raw contain
def read_df(container_path, file_name):
    output_df = spark.read.parquet(f"{container_path}/{file_name}")
    return output_df

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
  column_list = []
  for column_name in input_df.schema.names:
    if column_name != partition_column:
      column_list.append(column_name)
  column_list.append(partition_column)
  output_df = input_df.select(column_list)
  return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
  output_df = re_arrange_partition_column(input_df, partition_column)
  spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
  else:
    output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def df_column_to_list(input_df, column_name):
  df_row_list = input_df.select(column_name) \
                        .distinct() \
                        .collect()
  
  column_value_list = [row[column_name] for row in df_row_list]
  return column_value_list

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, merge_condition, partition_column, catalog_name):
    if(spark.catalog.tableExists(f"{catalog_name}.{db_name}.{table_name}")):
        # deltaTable = DeltaTable.forPath(sparkSession=spark, path=f"{folder_path}/{table_name}")

        # --- Had to adjust the deltaTable definition for the below since the path of managed tables in Unity Catalog has changed:
        # now we should be accessing the managed tables directly in spark because the folders are now grids rather than table names. we get an error if we try to access the files --- #.

        deltaTable = DeltaTable.forName(spark, f"{catalog_name}.{db_name}.{table_name}")
        deltaTable.alias("tgt") \
            .merge(
                input_df.alias("upd"), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        input_df.write \
            .mode("overwrite") \
            .partitionBy(partition_column) \
            .format("delta") \
            .saveAsTable(f"{catalog_name}.{db_name}.{table_name}")

