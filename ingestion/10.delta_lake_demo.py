# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 2. Write data to delta lake (external table)
# MAGIC 3. Read data from delta lake (Table)
# MAGIC 4. Read data from delta lake (File)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS hive_metastore.f1_demo
# MAGIC MANAGED LOCATION '/mnt/formulaonedb/demo'

# COMMAND ----------

results_df = spark.read \
    .option("inferSchema", True) \
    .json("/mnt/formulaonedb/raw/2021-03-28/results.json")

# COMMAND ----------

# Managed table
results_df.write.format("delta").mode("overwrite").saveAsTable("hive_metastore.f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.f1_demo.results_managed

# COMMAND ----------

# External table - first we create the file in the external location in the container
results_df.write.format("delta").mode("overwrite").save("/mnt/formulaonedb/demo/results_external")
# External table - then we create the external table BELOW:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE hive_metastore.f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/formulaonedb/demo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.f1_demo.results_external

# COMMAND ----------

# Again, what if you didn't want to create a table but just read the file
results_external_df = spark.read.format("delta").load("/mnt/formulaonedb/demo/results_external")

# COMMAND ----------

# we can also add the partition as we did before
results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("hive_metastore.f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS hive_metastore.f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Update Delta Table
# MAGIC 2. Delete from Delta table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE hive_metastore.f1_demo.results_managed
# MAGIC   SET points = 11 - position
# MAGIC WHERE position <= 10
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.f1_demo.results_managed

# COMMAND ----------

# The python equivalent
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/formulaonedb/demo/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.update(
  condition = "position <=10",
  set = { "points": "21 - position" }
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM hive_metastore.f1_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# the python equal
deltaTable = DeltaTable.forPath(spark, '/mnt/formulaonedb/demo/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.delete(
  condition = "points = 0"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC Upsert using merge

# COMMAND ----------

# in Upsert you do all 3 in one command - insert new records, update or delete existing records if applicable.
drivers_day1_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formulaonedb/raw/2021-03-28/drivers.json") \
.filter("driverId <= 10") \
.select("driverId", "dob", "name.forename", "name.surname")
display(drivers_day1_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")


# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formulaonedb/raw/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 6 AND 15") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))
display(drivers_day2_df)

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formulaonedb/raw/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))
display(drivers_day3_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.f1_demo.drivers_merge (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC Day 1
# MAGIC - We insert all new records becuase the table was just created 

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO hive_metastore.f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.dob = upd.dob,
# MAGIC     tgt.forename = upd.forename,
# MAGIC     tgt.surname = upd.surname,
# MAGIC     tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     createdDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     current_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC Day 2
# MAGIC - In day 2 we got data from 6 to 10 as updates and we insert 11 to 15

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO hive_metastore.f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     dob = upd.dob,
# MAGIC     forename = upd.forename,
# MAGIC     surname = upd.surname,
# MAGIC     updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     createdDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     current_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC Day 3
# MAGIC - for this one we didn't create temp table because we are going to do using pyspark
# MAGIC - In day 3 we got data from 1 to 5 as updates and we insert 16 to 20 will be an insert

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

deltaTable = DeltaTable.forPath(spark, '/mnt/formulaonedb/demo/drivers_merge')

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
  .whenMatchedUpdate(set =
    {
        "driverId": "upd.driverId",
        "dob" : "upd.dob",
        "forename" : "upd.forename",
        "surname" :  "upd.surname",
    "updatedDate" :  "current_timestamp()"
    }
  ) \
  .whenNotMatchedInsert(values = 
    {
    "driverId": "upd.driverId",
    "dob" :  "upd.dob",
    "forename" : " upd.forename",
    "surname" :  "upd.surname",
    "createdDate" :  "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC 1. History & Versioning
# MAGIC 2. Time Travel
# MAGIC 3. Vaccum
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESC HISTORY hive_metastore.f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC -- in order to get different versions: 
# MAGIC SELECT * FROM hive_metastore.f1_demo.drivers_merge VERSION AS OF 2
# MAGIC -- another option SELECT * FROM delta.`/mnt/formulaonedb/demo/drivers_merge` VERSION AS OF 2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- to get the timestamp of the version
# MAGIC SELECT * FROM hive_metastore.f1_demo.drivers_merge TIMESTAMP AS OF "2024-09-04T14:34:40.000+00:00"

# COMMAND ----------

# in order to due this in pyspark style
df = spark.read.format("delta").option("timestampAsOf", "2024-09-04T14:34:40.000+00:00").load("/mnt/formulaonedb/demo/drivers_merge")
display(df)

# COMMAND ----------

# in order to deal wiht people who asked to remove their data (we have 30 days after the request) we can use Vacuum:
# usually vaccum removes the history which is older than 7 days, but that can also be changed.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We have to set this parameter and 0 hours to remove immediatelly the data.
# MAGIC -- SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC -- VACUUM hive_metastore.f1_demo.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.f1_demo.drivers_merge VERSION AS OF 2

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM hive_metastore.f1_demo.drivers_merge WHERE driverId = 1;
# MAGIC
# MAGIC --Example of issue in one version of the the data, we'll delete data for example
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.f1_demo.drivers_merge VERSION AS OF 4;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- we can fix it with the merge statement
# MAGIC MERGE INTO hive_metastore.f1_demo.drivers_merge AS tgt
# MAGIC USING hive_metastore.f1_demo.drivers_merge VERSION AS OF 3 src
# MAGIC   ON (tgt.driverId = src.driverId)
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY hive_metastore.f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- scenario where we have a table that we'd like to change to delta    
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.f1_demo.drivers_convert_to_delta (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC -- we can now insert some data from our delta table drivers_merge into the parquet table
# MAGIC INSERT INTO hive_metastore.f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM hive_metastore.f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA hive_metastore.f1_demo.drivers_convert_to_delta

# COMMAND ----------

# we can also convert it to delta from a file system like a parquet file
# first we read the table we just created
df = spark.table("hive_metastore.f1_demo.drivers_convert_to_delta")

# COMMAND ----------

# now we right into the parquet file (_new)
df.write.format("parquet").save("/mnt/formulaonedb/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- we can now convert this to delta table
# MAGIC CONVERT TO DELTA parquet.`/mnt/formulaonedb/demo/drivers_convert_to_delta_new`
