-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Create or Replace & CTAS
-- MAGIC 1. Difference between Create or Replace and Drop and Create Table statements
-- MAGIC 2. CTAS statement
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Difference between Create or Replace and Drop and Create Table statements
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Behaviour of the DROP and CREATE statements

-- COMMAND ----------

DROP TABLE IF EXISTS swathy_catalog.demo_db.companies;

CREATE TABLE swathy_catalog.demo_db.companies
  (company_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
   company_name STRING,
   founded_date DATE,
   country      STRING);

INSERT INTO swathy_catalog.demo_db.companies 
(company_name, founded_date, country)
VALUES ("Apple", "1976-04-01", "USA"),
       ("Microsoft", "1975-04-04", "USA"),
       ("Google", "1998-09-04", "USA"),
       ("Amazon", "1994-07-05", "USA"),
       ("Tencent", "1998-11-11", "China");   

-- COMMAND ----------

DESC HISTORY swathy_catalog.demo_db.companies;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Behaviour of the CREATE OR REPLACE statement

-- COMMAND ----------

DROP TABLE IF EXISTS swathy_catalog.demo_db.companies;

-- COMMAND ----------

CREATE OR REPLACE TABLE swathy_catalog.demo_db.companies
  (company_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
   company_name STRING,
   founded_date DATE,
   country      STRING);

INSERT INTO swathy_catalog.demo_db.companies 
(company_name, founded_date, country)
VALUES ("Apple", "1976-04-01", "USA"),
       ("Microsoft", "1975-04-04", "USA"),
       ("Google", "1998-09-04", "USA"),
       ("Amazon", "1994-07-05", "USA"),
       ("Tencent", "1998-11-11", "China");   

-- COMMAND ----------

DESC HISTORY swathy_catalog.demo_db.companies;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. CTAS statement

-- COMMAND ----------

DROP TABLE IF EXISTS swathy_catalog.demo_db.companies_china;

-- COMMAND ----------

CREATE OR REPLACE TABLE swathy_catalog.demo_db.companies
  (company_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
   company_name STRING,
   founded_date DATE,
   country      STRING);

INSERT INTO swathy_catalog.demo_db.companies 
(company_name, founded_date, country)
VALUES ("Apple", "1976-04-01", "USA"),
       ("Microsoft", "1975-04-04", "USA"),
       ("Google", "1998-09-04", "USA"),
       ("Amazon", "1994-07-05", "USA"),
       ("Tencent", "1998-11-11", "China");   

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

desc table swathy_catalog.demo_db.companies;

-- COMMAND ----------

create table swathy_catalog.demo_db.companies_china
as
select *
from swathy_catalog.demo_db.companies
where country = 'China';

-- COMMAND ----------

create table swathy_catalog.demo_db.companies_china
as
select cast(company_id as int) as company_id,
       company_name,
       founded_date,
       country
from swathy_catalog.demo_db.companies
where country = 'China';

-- COMMAND ----------



-- COMMAND ----------

ALTER TABLE swathy_catalog.demo_db.companies_china 
ALTER column company_id set NOT NULL;

-- COMMAND ----------

DESC HISTOry swathy_catalog.demo_db.companies_china;

-- COMMAND ----------

CREATE OR REPLACE TABLE order_tab AS
SELECT *
FROM parquet.`/Volumes/workspace/default/databricks-1/orders.parquet`;

-- COMMAND ----------

select * from order_tab;

-- COMMAND ----------

CREATE OR REPLACE TABLE order_tab_csv using delta AS
SELECT *
FROM csv.`/Volumes/workspace/default/databricks-1/orders.csv`;

-- COMMAND ----------

select * from order_tab_csv;

-- COMMAND ----------

create or replace table order_tab_new using delta as
select *
from read_files("/Volumes/workspace/default/databricks-1/orders.csv",
                format => 'csv',
                sep => ',',
                header => true,
                mode => "FAILFAST");
