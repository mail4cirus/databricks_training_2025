# Databricks notebook source
spark

# COMMAND ----------

user_data = ([(1,'Naveen'),(2,'karan')])
user_schema = "id int, name string"

# COMMAND ----------



df = spark.createDataFrame(data=user_data,schema=user_schema)

# COMMAND ----------

df.display()


# COMMAND ----------

df.show()

# COMMAND ----------

Lazy Evaluation
Transformation and Actions

Transgormations
    select
    filter
    sort
    group
    agg
    join



Action
    show
    display

# COMMAND ----------

df.select("*")
# df.select("id")
#just it will create a plan and show this as a output DataFrame[id: int, name: string]. It wont give the results unless and until we use action. Also time taken is also very less than 1 sec(0.14 sec) since it is just a plan

# COMMAND ----------

df.select("*").show()

#this is action and will create a spark job and the dataframe and gives results. Now time is also more than the plan (1.16 sec) since the job is created

# COMMAND ----------

df.select("id".alias("emp_id"))
# above ones give the error since id is consider as string not a column .AttributeError: 'str' object has no attribute 'alias'



# COMMAND ----------

from pyspark.sql.functions import *

df.select(col("id").alias("emp_id")).show()

# COMMAND ----------

df.withColumnRenamed("id","emp_id").show()

# COMMAND ----------

df.withColumnsRenamed({"id":"emp_id","name":"emp_name"}).show()

# COMMAND ----------

df2 = df.withColumn("ingestion_date",current_date())

# COMMAND ----------

df2.display()
