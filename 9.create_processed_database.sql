-- Databricks notebook source
-- he creates the databases to later creates tables so daa analysts can interact with it using SQL.

-- COMMAND ----------

-- Create bronze External Location
CREATE EXTERNAL LOCATION IF NOT EXISTS `mnt_formulaonedb_raw`
URL 'abfss://raw@formulaonedb.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `formulaonedb_containers`)
COMMENT 'the external location is an object that combines the storage credential with the storage account container (URL the path for where you create the external location)';

-- COMMAND ----------

-- Create silver External Location
CREATE EXTERNAL LOCATION IF NOT EXISTS `mnt_formulaonedb_processed`
URL 'abfss://processed@formulaonedb.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `formulaonedb_containers`)
COMMENT 'the external location is an object that combines the storage credential with the storage account container (URL the path for where you create the external location)';

-- COMMAND ----------

-- Create gold External Location
CREATE EXTERNAL LOCATION IF NOT EXISTS `mnt_formulaonedb_presentation`
URL 'abfss://presentation@formulaonedb.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `formulaonedb_containers`)
COMMENT 'the external location is an object that combines the storage credential with the storage account container (URL the path for where you create the external location)';

-- COMMAND ----------

-- have to specify the location
DROP SCHEMA IF EXISTS databricks_ws_2.f1_processed;
DROP SCHEMA IF EXISTS databricks_ws_2.f1_bronze;
DROP SCHEMA IF EXISTS databricks_ws_2.f1_gold;

-- COMMAND ----------

-- we can now ceate the schemas in the catalogue
CREATE SCHEMA IF NOT EXISTS databricks_ws_2.f1_bronze
MANAGED LOCATION "abfss://raw@formulaonedb.dfs.core.windows.net/";

CREATE SCHEMA IF NOT EXISTS databricks_ws_2.f1_silver
MANAGED LOCATION "abfss://processed@formulaonedb.dfs.core.windows.net/";

CREATE SCHEMA IF NOT EXISTS databricks_ws_2.f1_gold
MANAGED LOCATION "abfss://presentation@formulaonedb.dfs.core.windows.net/";

-- COMMAND ----------

desc schema databricks_ws_2.f1_bronze;

-- COMMAND ----------

SELECT current_metastore()
