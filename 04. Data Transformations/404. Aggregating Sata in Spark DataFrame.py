# Databricks notebook source
# DBTITLE 1,Create Dataframe
import datetime

emp_schema = ['empno', "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno"]

emp_data = [ 
            (7839, 'KING', 'PRESIDENT', None, datetime.date(1981, 1, 17), 5000, None, 10),
            (7698, 'BLAKE', 'MANAGER', 7839, datetime.date(1981, 5, 1), 2850, None, 30),
            (7782, 'CLARK', 'MANAGER', 7839, datetime.date(1981, 6, 9), 2450, None, 10),
            (7566, 'JONES', 'MANAGER', 7839, datetime.date(1981, 4, 2), 2975, None, 20),
            (7788, 'SCOTT', 'ANALYST', 7566, datetime.date(1981, 12, 9), 3000, None, 20),
            (7902, 'FORD', 'ANALYST', 7566, datetime.date(1981, 12, 3), 3000, None, 20),
            (7369, 'SMITH', 'CLERK', 7902, datetime.date(1988, 12, 17), 800, None, 20),
            (7499, 'ALLEN', 'SALESMAN', 7698, datetime.date(1981, 2, 20), 1600, 300, 30),
            (7521, 'WARD', 'SALESMAN', 7698, datetime.date(1981, 2, 22), 1250, 500, 30),
            (7654, 'MARTIN', 'SALESMAN', 7698, datetime.date(1981, 9, 28), 1250, 1400, 30),
            (7844, 'TURNER', 'SALESMAN', 7698, datetime.date(1981, 9, 8), 1500, 0, 30),
            (7876, 'ADAMS', 'CLERK', 7788, datetime.date(1983, 1, 12), 1100, None, 20),
            (7900, 'JAMES', 'CLERK', 7698, datetime.date(1981, 12, 3), 950, None, 30),
            (7934, 'MILLER', 'CLERK', 7782, datetime.date(1982, 1, 23), 1300, None, 10)
           ]

emp_df = spark.createDataFrame(data=emp_data, schema=emp_schema)
emp_df.show()

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Total aggregation
emp_df.select(F.count('*')).show()   

# COMMAND ----------

# DBTITLE 1,Grouped aggregation or by-key aggregation
emp_df.groupBy('deptno').agg(F.count('*').show()  

# COMMAND ----------

# DBTITLE 1,Filter, select, aggregation, alias
# get salary summation for a given deptno

emp_df.filter('deptno = 10').select(F.sum('sal').alias('Total_sal_for_dept_10')).show()

# COMMAND ----------

# DBTITLE 1,Multiple aggregation functions

emp_df.groupBy('deptno').agg(F.sum('sal').alias('Total_sal_for_dept')).show()

# COMMAND ----------

# DBTITLE 1,groupBy and agg
# get department wise summary
(emp_df
 .groupBy('deptno')
 .agg(
     F.count('sal').alias('emp_count'),
     F.sum('sal').alias('Total_sal'),
     F.avg('sal').alias('Avg_sal'),
     F.min('sal').alias('min_sal'),
     F.max('sal').alias('max_sal'),
     F.stddev('sal').alias('stddev_sal'),
     F.variance('sal').alias('variance'),
 )).show()

# COMMAND ----------

emp_df.filter("job = 'MANAGER'").select(F.avg('sal').alias('Avg_sal_for_managers')).show()

# COMMAND ----------

emp_df.filter(emp_df.hiredate > datetime.date(1981, 12, 31)).select(F.count('*').alias('count_ppl_joined_after_1981')).show()

# COMMAND ----------

emp_df.groupBy('deptno', 'job').agg(
    F.count('*').alias('emp_count'),
    F.sum('sal').alias('Total_sal')
).orderBy("deptno").show()

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ===================================== END ========================================
