-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## History and Time Travel
-- MAGIC 1. Query Delta Lake table history
-- MAGIC 1. Query previous versions of the data
-- MAGIC 1. Query data from a specific time. 
-- MAGIC 1. Restore data to a specific version.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1. Query Delta Lake Table History

-- COMMAND ----------

DESCRIBE HISTORY swathy_catalog.demo_db.companies;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2. Query Data from a Specific Version

-- COMMAND ----------

SELECT * FROM swathy_catalog.demo_db.companies;

-- COMMAND ----------

SELECT * FROM swathy_catalog.demo_db.companies
VERSION AS OF 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3. Query Data from a Specific Time

-- COMMAND ----------

SELECT * FROM swathy_catalog.demo_db.companies
TIMESTAMP AS OF '2025-08-26T05:17:10.000+00:00';


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 4. Restore Data in the Table to a Specific Version

-- COMMAND ----------

RESTORE TABLE swathy_catalog.demo_db.companies VERSION AS OF 1;

-- COMMAND ----------

SELECT * FROM swathy_catalog.demo_db.companies;

-- COMMAND ----------

DESC HISTORY swathy_catalog.demo_db.companies;
