# Databricks notebook source
# DBTITLE 1,Cleanup
dbutils.fs.rm('/Volumes/workspace/default/databricks-1/orders_partitioned_by_date', recurse=True)
dbutils.fs.rm('/Volumes/workspace/default/databricks-1/orders_partitioned_by_month', recurse=True)
dbutils.fs.rm('/Volumes/workspace/default/databricks-1/orders_partitioned', recurse=True)

# COMMAND ----------

# DBTITLE 1,Read JSON file into DataFrame
df = spark.read.json('/Volumes/workspace/default/databricks-1/orders_data.json')

# COMMAND ----------

# DBTITLE 1,What are the columns and their datatypes?
df.printSchema()

# COMMAND ----------

# DBTITLE 1,How data looks like?


# COMMAND ----------

# DBTITLE 1,How many records in Dataframe?
df.count()

# COMMAND ----------

# DBTITLE 1,partitionBy Documentation


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Partitioning by single column

# COMMAND ----------

# DBTITLE 1,Preparing column for splitting data
from pyspark.sql.functions import col, date_format

df = (df.withColumn('order_date', col('order_date').cast('timestamp'))
      .withColumn('order_dt', date_format('order_date', 'yyyyMMdd'))
      .withColumn('order_month', date_format('order_date', 'yyyyMM'))
      .coalesce(1)
)

# COMMAND ----------

# DBTITLE 1,see updated dataframe
df.show(5)

# COMMAND ----------

# DBTITLE 1,Write Dataframe into parquet file by partitioning
# partioning data by date

df.write.partitionBy('order_date').\
    parquet('/Volumes/workspace/default/databricks-1/ordersPartitionedByDate')


# COMMAND ----------

# DBTITLE 1,Get records count from written parquet file
# verify the partitioned data

spark.read.parquet('/Volumes/workspace/default/databricks-1/ordersPartitionedByDate').count()

# COMMAND ----------

# DBTITLE 1,Partitioning by month
# partition data by order month
df.coalesce(1).write.partitionBy('order_month').\
    parquet('/Volumes/workspace/default/databricks-1/ordersPartitionedByMonth')

# COMMAND ----------

# DBTITLE 1,how many records?
spark.read.parquet('/Volumes/workspace/default/databricks-1/ordersPartitionedByMonth').count()

# COMMAND ----------

# DBTITLE 1,Remove written directories
dbutils.fs.rm('/Volumes/workspace/default/databricks-1/ordersPartitionedByDate', recurse=True)
dbutils.fs.rm('/Volumes/workspace/default/databricks-1/ordersPartitionedByMonth', recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Partitioning data by multiple columns
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Prepare data by adding required columns
df = (df.withColumn('year', date_format('order_date', 'yyyy'))
        .withColumn('month', date_format('order_date', 'MM'))
        .withColumn('day_of_month', date_format('order_date', 'dd'))
)

# COMMAND ----------

# DBTITLE 1,See Data
df.show(5)

# COMMAND ----------

# DBTITLE 1,Partition data by year, month and day_of_month
(df.coalesce(1)
    .write
    .mode('ignore')
    .partitionBy('year', 'month', 'day_of_month')
    .parquet('/Volumes/workspace/default/databricks-1/data/orders_partitioned')
)

# COMMAND ----------

# DBTITLE 1,Get count from written files
spark.read.parquet('/Volumes/workspace/default/databricks-1/data/orders_partitioned').count()

# COMMAND ----------

# DBTITLE 1,How many records in 2013
spark.read.parquet('/Volumes/workspace/default/databricks-1/data/orders_partitioned').filter('year = 2013').count()

# COMMAND ----------

# DBTITLE 1,How many records in 2014
spark.read.parquet('/Volumes/workspace/default/databricks-1/data/orders_partitioned').filter('year = 2014').count()

# COMMAND ----------

# DBTITLE 1,How many records in 2013: Alternate
spark.read.parquet('/Volumes/workspace/default/databricks-1/data/orders_partitioned/year=2013').count()

# COMMAND ----------

# DBTITLE 1,How many records in 2014: Alternate
spark.read.parquet('/Volumes/workspace/default/databricks-1/data/orders_partitioned/year=2014').count()

# COMMAND ----------

# DBTITLE 1,Create View
df.createOrReplaceTempView('orders_data')

# COMMAND ----------

# DBTITLE 1,Verify view creation
spark.sql('SHOW tables').show()

# COMMAND ----------

# DBTITLE 1,Spark-SQL: Get count from view for 2013 records
spark.sql(
"""
SELECT *
FROM orders_data
WHERE year = 2013
"""
).show()

# COMMAND ----------

# DBTITLE 1,SQL Syntax
# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM orders_data
# MAGIC WHERE year = 2013;

# COMMAND ----------

# DBTITLE 1,Remove Created Directories
dbutils.fs.rm('/Volumes/workspace/default/databricks-1/data', recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ============================================= END ==========================================
