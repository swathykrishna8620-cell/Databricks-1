# Databricks notebook source
# use personal compute cluster for this task (each with 2 core and 8GB RAM is sufficient) not required as we use serverless

# COMMAND ----------

# DBTITLE 1,Import necessary libraries
import dlt
import pyspark.sql.functions as F

# COMMAND ----------

schema = """
order_id	int,
customer_id	int,
product_id	int,
quantity	int,
price	float,
order_date	date,
status	string
"""

# COMMAND ----------

# Task 1: Ingest Data into Bronze layer

# @dlt is what creates the table
file_path="/Volumes/swathy_catalog/swathy_schema/swathy_volume/ecommerce_data1.csv"
order_rules={
    "rule_1":"order_id IS NOT NULL",
    "rule_2":"price>0"
}
@dlt.table(
    comment='Bronze Table containing raw e-commerce data'
)

@dlt.expect_all_or_drop(order_rules)

def sample_ecomm_bronze_data():
    return (
        spark.read.format("csv")
            .option("header", 'true')
            .schema(schema)
            .load(file_path)
    )

# COMMAND ----------

# Task 2: Clean and Transform Data to silver layer

@dlt.table(
    comment = "Silver table with cleaned and validated e-commerce data"
)

def sample_ecomm_silver_data():
    bronze_df = dlt.read("sample_ecomm_bronze_data")
    return (
        bronze_df
        .filter(F.col("Status").isNotNull())
        .withColumn("total_price", F.col('Quantity') * F.col('Price'))
    )

# COMMAND ----------

# Task 3: Aggregate data to create Gold layer

@dlt.table(
    comment = "Gold table with aggregated e-commerce data"
    )
def sample_ecomm_gold_data():
    silver_df = dlt.read("sample_ecomm_silver_data")
    return (
        silver_df
        .groupBy("product_id")
        .agg(
            F.sum("Quantity").alias("total_quantity_sold"),
            F.sum("total_price").alias("total_sales"),
            F.avg("price").alias("avg_price")
            )
        )

# COMMAND ----------

#task 4: Filter and enrich data fro reporting

@dlt.table(
    comment = "Gold table filtered for active sales data"
)
def sample_gold_active_sales_data():
    gold_df = dlt.read("sample_ecomm_gold_data")
    return gold_df.filter(F.col("total_quantity_sold") > 10) 

# COMMAND ----------



# COMMAND ----------


