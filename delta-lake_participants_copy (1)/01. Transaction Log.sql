-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Understanding Delta Lake Transaction Log
-- MAGIC Understand the cloud storage directory structure behind delta lake tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 0. Create a new schema under the demo catalog for this section of the course (delta_lake)

-- COMMAND ----------

create schema if not exists swathy_catalog.demo_db


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1. Create a Delta Lake Table

-- COMMAND ----------

create table if not exists swathy_catalog.demo_db.companies
(company_name string,
founded_date date,
country string);

-- COMMAND ----------

-- DBTITLE 1,describe table
desc extended swathy_catalog.demo_db.companies;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2. Insert some data

-- COMMAND ----------

insert into swathy_catalog.demo_db.companies
values("Apple","1976-04-01","USA");

-- COMMAND ----------

select * from swathy_catalog.demo_db.companies;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3. Insert some more data

-- COMMAND ----------

insert into swathy_catalog.demo_db.companies
values("Microsoft","1975-04-04","USA"),
  ("Google","1998-09-04","USA"),
  ("Amazon","1974-07-05","USA");

