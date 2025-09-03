-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Insert Overwrite
-- MAGIC 1. Replace all the data in a table
-- MAGIC 1. Replace all the data from a specific partition
-- MAGIC 1. How to handle schema changes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC INSERT OVERWITE - Overwrites the existing data in a table or a specific partition with the new data. 
-- MAGIC
-- MAGIC INSERT INTO - Appends new data
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Replace all the data in a table

-- COMMAND ----------

DROP TABLE IF EXISTS swathy_catalog.demo_db.gold_companies;

CREATE TABLE swathy_catalog.demo_db.gold_companies
  (company_name STRING,
   founded_date DATE,
   country      STRING);

INSERT INTO swathy_catalog.demo_db.gold_companies 
(company_name, founded_date, country)
VALUES ("Apple", "1976-04-01", "USA"),  
       ("Tencent", "1998-11-11", "China"); 

SELECT * FROM swathy_catalog.demo_db.gold_companies;        

-- COMMAND ----------

DROP TABLE IF EXISTS swathy_catalog.demo_db.bronze_companies;

CREATE TABLE swathy_catalog.demo_db.bronze_companies
  (company_name STRING,
   founded_date DATE,
   country      STRING);

INSERT INTO swathy_catalog.demo_db.bronze_companies 
(company_name, founded_date, country)
VALUES ("Apple", "1976-04-01", "USA"),
       ("Microsoft", "1975-04-04", "USA"),
       ("Google", "1998-09-04", "USA"),
       ("Amazon", "1994-07-05", "USA"),
       ("Tencent", "1998-11-11", "China");   

SELECT * FROM swathy_catalog.demo_db.bronze_companies;       

-- COMMAND ----------

insert into table swathy_catalog.demo_db.gold_companies
select * from swathy_catalog.demo_db.bronze_companies

-- COMMAND ----------

select * from swathy_catalog.demo_db.gold_companies

-- COMMAND ----------

DROP TABLE IF EXISTS swathy_catalog.demo_db.gold_companies;

CREATE TABLE swathy_catalog.demo_db.gold_companies
  (company_name STRING,
   founded_date DATE,
   country      STRING);

INSERT INTO swathy_catalog.demo_db.gold_companies 
(company_name, founded_date, country)
VALUES ("Apple", "1976-04-01", "USA"),  
       ("Tencent", "1998-11-11", "China"); 

SELECT * FROM swathy_catalog.demo_db.gold_companies;   

-- COMMAND ----------

insert overwrite table swathy_catalog.demo_db.gold_companies
select * from swathy_catalog.demo_db.bronze_companies;

-- COMMAND ----------

select * from swathy_catalog.demo_db.gold_companies

-- COMMAND ----------

desc history swathy_catalog.demo_db.gold_companies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Replace all the data from a specific partition

-- COMMAND ----------

DROP TABLE IF EXISTS swathy_catalog.demo_db.gold_companies_partitioned;

CREATE TABLE swathy_catalog.demo_db.gold_companies_partitioned
  (company_name STRING,
   founded_date DATE,
   country      STRING)
PARTITIONED BY (country);

INSERT INTO swathy_catalog.demo_db.gold_companies_partitioned 
(company_name, founded_date, country)
VALUES ("Apple", "1976-04-01", "USA"),  
       ("Tencent", "1998-11-11", "China"); 

SELECT * FROM swathy_catalog.demo_db.gold_companies_partitioned;        

-- COMMAND ----------

DESC EXTENDED swathy_catalog.demo_db.gold_companies_partitioned

-- COMMAND ----------

DROP TABLE IF EXISTS swathy_catalog.demo_db.bronze_companies_usa;

CREATE TABLE swathy_catalog.demo_db.bronze_companies_usa
  (company_name STRING,
   founded_date DATE,
   country      STRING);

INSERT INTO swathy_catalog.demo_db.bronze_companies_usa 
(company_name, founded_date, country)
VALUES ("Apple", "1976-04-01", "USA"),
       ("Microsoft", "1975-04-04", "USA"),
       ("Google", "1998-09-04", "USA"),
       ("Amazon", "1994-07-05", "USA");   

SELECT * FROM swathy_catalog.demo_db.bronze_companies_usa;       

-- COMMAND ----------

insert overwrite table swathy_catalog.demo_db.gold_companies_partitioned
partition (country = "USA")
select company_name,
        founded_date
    from swathy_catalog.demo_db.bronze_companies_usa

-- COMMAND ----------

SELECT * FROM swathy_catalog.demo_db.gold_companies_partitioned;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. How to handle schema changes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Insert Overwrite -> Use to overwrite the data in a table or a partition when there are no schema changes. 
-- MAGIC
-- MAGIC Insert overwrite does not support if schemas of both tables are different 
-- MAGIC
-- MAGIC Create or replace table -> Use when there are schema changes. 
-- MAGIC

-- COMMAND ----------

drop table if exists customers;
create table customers(
  customer_id int,
  cust_name string,
  email string,
  city string
);


-- COMMAND ----------

insert overwrite table customers
select customer_id, cust_name, email, city 
from json.`/Volumes/workspace/default/databricks-1/customers1.json`
