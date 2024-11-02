# Databricks notebook source
# MAGIC %md
# MAGIC This is the same as race_results notebook but showing how the implementation of the incremental load can be done\
# MAGIC using the SQL API.\
# MAGIC This also has a difference in how the points are calculated - calculated_points column, but this is it.
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- I changed the default catalog to hive_metastore
# MAGIC select current_catalog()

# COMMAND ----------

# we add the 2 columns to account for versioning of the table (created_date, updated_date)
# the primary key a composed key of driver_id and race_id
spark.sql(f"""
              CREATE TABLE IF NOT EXISTS hive_metastore.f1_presentation.calculated_race_results
              (
              race_year INT,
              team_name STRING,
              driver_id INT,
              driver_name STRING,
              race_id INT,
              position INT,
              points INT,
              calculated_points INT,
              created_date TIMESTAMP,
              updated_date TIMESTAMP
              )
              USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW race_result_updated
            AS
            SELECT races.race_year,
                  constructor.name AS team_name,
                  drivers.driver_id,
                  drivers.name AS driver_name,
                  races.race_id,
                  results.position,
                  results.points,
                  11 - results.position AS calculated_points
            FROM f1_processed.results
            JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
            JOIN f1_processed.constructor ON (results.constructor_id = constructor.constructor_id)
            JOIN f1_processed.races ON (results.race_id = races.race_id)
            WHERE results.position <= 10
            AND results.file_date = '{v_file_date}'
""")

# COMMAND ----------

# MAGIC %sql SELECT * FROM race_result_updated LIMIT 10
# MAGIC -- check we get data back

# COMMAND ----------

# we can now do the merge statement to update ncessary records and insert new ones
spark.sql(f"""
              MERGE INTO f1_presentation.calculated_race_results tgt
              USING race_result_updated upd
              ON (tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id)
              WHEN MATCHED THEN
                UPDATE SET tgt.position = upd.position,
                           tgt.points = upd.points,
                           tgt.calculated_points = upd.calculated_points,
                           tgt.updated_date = current_timestamp
              WHEN NOT MATCHED
                THEN INSERT (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, created_date ) 
                     VALUES (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, current_timestamp)
       """)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- we should get the same count in the temp view as we get in the presentation layer
# MAGIC SELECT COUNT(1) FROM race_result_updated;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM f1_presentation.calculated_race_results;

# COMMAND ----------


