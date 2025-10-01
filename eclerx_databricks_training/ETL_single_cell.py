# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists formula1

# COMMAND ----------

from pyspark.sql.functions import *


input_path = "dbfs:/FileStore/eclerx_input_files/circuits.csv"

# df = spark.read.option("header",True).csv(input_path)

# all the columns read as string from the above df

# df = spark.read.option("header",True).option("inferSchema",True).csv(input_path)
# the below is same as above df
df = spark.read.csv(input_path,header=True,inferSchema=True)



df_final = df\
        .withColumnsRenamed({"circuitId":"circuit_id","circuitRef":"circuit_ref"})\
        .withColumn("ingestin_date",current_date())\
        .drop(col("url"))

df_final.write.mode("overwrite").saveAsTable("formula1.circuit")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from hive_metastore.formula1.circuit;
