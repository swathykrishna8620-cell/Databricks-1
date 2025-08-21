# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Dropping not required Columns

# COMMAND ----------

# DBTITLE 1,Create Sample Dataframe
from pyspark.sql import functions as F
from pyspark import Row
import datetime
users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "gender": "male",
        "current_city": "Dallas",
        "phone_numbers": Row(mobile="+1 234 567 8901", home="+1 234 567 8911"),
        "courses": [1, 2],
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "gender": "male",
        "current_city": "Houston",
        "phone_numbers":  Row(mobile="+1 234 567 8923", home="1 234 567 8934"),
        "courses": [3],
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "gender": "female",
        "current_city": "",
        "phone_numbers": Row(mobile="+1 714 512 9752", home="+1 714 512 6601"),
        "courses": [2, 4],
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "gender": "male",
        "current_city": "San Fransisco",
        "phone_numbers": Row(mobile=None, home=None),
        "courses": [],
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "gender": "female",
        "current_city": None,
        "phone_numbers": Row(mobile="+1 817 934 7142", home=None),
        "courses": [],
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]


users_df = spark.createDataFrame(users)
users_df.show()

# COMMAND ----------

# DBTITLE 1,Selecting Everything from Dataframe
users_df.select('*').show()

# COMMAND ----------

# DBTITLE 1,Dropping a column from Dataframe
users_df.drop('last_updated_ts').show()

# COMMAND ----------

# DBTITLE 1,Just another way
users_df.drop(users_df['last_updated_ts']).show() #only changes in show()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

users_df.columns

# COMMAND ----------

# DBTITLE 1,One more way
users_df.drop(F.col('last_updated_ts')).show()

# COMMAND ----------

# DBTITLE 1,Try Removing invalid column
# do nothing, no error as well

users_df.drop('column_does_not_exist').show()

# COMMAND ----------

# DBTITLE 1,Dropping multiple columns
users_df.drop('first_name','last_name','last_updated_ts').show()

# another two syntaxes will also work similarly for multiple columns removal

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Dealing with duplicates

# COMMAND ----------

# DBTITLE 1,Creating Dataframe
users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "gender": "male",
        "current_city": "Dallas",
        "phone_numbers": Row(mobile="+1 234 567 8901", home="+1 234 567 8911"),
        "courses": [1, 2],
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "gender": "male",
        "current_city": "Houston",
        "phone_numbers":  Row(mobile="+1 234 567 8923", home="1 234 567 8934"),
        "courses": [3],
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "gender": "male",
        "current_city": "Houston",
        "phone_numbers":  Row(mobile="+1 234 567 8923", home="1 234 567 8934"),
        "courses": [3],
        "is_customer": True,
        "amount_paid": 1050.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "gender": "female",
        "current_city": "",
        "phone_numbers": Row(mobile="+1 714 512 9752", home="+1 714 512 6601"),
        "courses": [2, 4],
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "gender": "female",
        "current_city": "",
        "phone_numbers": Row(mobile="+1 714 512 9752", home="+1 714 512 6601"),
        "courses": [2, 4],
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "gender": "male",
        "current_city": "San Fransisco",
        "phone_numbers": Row(mobile=None, home=None),
        "courses": [],
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "gender": "male",
        "current_city": "San Fransisco",
        "phone_numbers": Row(mobile=None, home=None),
        "courses": [],
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "gender": "female",
        "current_city": None,
        "phone_numbers": Row(mobile="+1 817 934 7142", home=None),
        "courses": [],
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]


users_df = spark.createDataFrame(users)
users_df.show()

# COMMAND ----------

# DBTITLE 1,Get the count of records
users_df.count()

# COMMAND ----------

# DBTITLE 1,Get the count of distinct records
users_df.distinct().count()

# COMMAND ----------

# DBTITLE 1,Dropping duplicate records
users_df.dropDuplicates().show()

#for Nikolaus amount_paid is different in 2 diff. records

# COMMAND ----------

# DBTITLE 1,Dropping duplicate specific to mentioned records
users_df.dropDuplicates(['id','first_name','last_name']).show()

# COMMAND ----------

# DBTITLE 1,drop_duplicates() Function
users_df.drop_duplicates(['id','first_name','last_name']).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dropping records based on Nulls
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create Dataframe
import datetime
users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": None,
        "first_name": None,
        "last_name": None,
        "email": None,
        "is_customer": None,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": None
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    },
    {
        "id": None,
        "first_name": None,
        "last_name": None,
        "email": None,
        "is_customer": None,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": None
    },
    {
        "id": 5,
        "first_name": None,
        "last_name": None,
        "email": None,
        "is_customer": None,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": None
    },
    {
        "id": None,
        "first_name": None,
        "last_name": None,
        "email": "nbrewitt1@dailymail.co.uk",
        "is_customer": None,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": None
    },
    {
        "id": None,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": None,
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "is_customer": True,
        "amount_paid": 1050.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 25, 3, 33, 0)
    }
]


import pandas as pd
users_df = spark.createDataFrame(pd.DataFrame(users))

users_df.show()

# COMMAND ----------

# DBTITLE 1,Drop the row if at least one null value in it

users_df.dropna().show()

# COMMAND ----------

# DBTITLE 1,Just another way
users_df.na.drop(how='any').show()

# COMMAND ----------

# DBTITLE 1,Drop if all the values in row are null
users_df.na.drop().show()


# COMMAND ----------

# DBTITLE 1,Drop if any of the value in row is null
users_df.na.drop(how='any').show()

# COMMAND ----------

# DBTITLE 1,Threshold
users_df.na.drop(thresh=2).show()

# COMMAND ----------

# DBTITLE 1,Subset with all null
users_df.na.drop(how='all',subset=['id','email']).show()

# COMMAND ----------

# DBTITLE 1,Subset with any null
users_df.na.drop(how='any',subset=['id','email']).show()

# COMMAND ----------

users_df.na.fill({"amount_paid":0}).show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

users_df1 = users_df.withColumn("amount_paid", col("amount_paid").cast("string"))
users_df1.na.fill({"amount_paid": 'Not Paid'}).show()

# COMMAND ----------

users_df2 = users_df.withColumn("first_name", col("first_name").cast("string"))
users_df.na.replace('Nikolaus','Nicolas',subset=['first_name']).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ============================================ END ==========================================
