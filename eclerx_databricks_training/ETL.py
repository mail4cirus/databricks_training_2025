# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/eclerx_input_files/
# MAGIC
# MAGIC

# COMMAND ----------

input_path = "dbfs:/FileStore/eclerx_input_files/circuits.csv"

# df = spark.read.option("header",True).csv(input_path)

# all the columns read as string from the above df

# df = spark.read.option("header",True).option("inferSchema",True).csv(input_path)
# the below is same as above df
df = spark.read.csv(input_path,header=True,inferSchema=True)


# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC - Task : 
# MAGIC -     1.rename circuitId - ciruit_id , circuitRef - circuit_ref
# MAGIC -     2. add new column with current_date
# MAGIC -     3.drop url col

# COMMAND ----------

from pyspark.sql.functions import *

df_final = df\
        .withColumnsRenamed({"circuitId":"circuit_id","circuitRef":"circuit_ref"})\
        .withColumn("ingestin_date",current_date())\
        .drop(col("url"))

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists formula1;
# MAGIC DROP TABLE IF EXISTS formula1.circuit;

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/formula1.db/circuit/_delta_log", recurse=True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/formula1.db/circuit", recurse=True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/formula1.db", recurse=True)


# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/formula1.db/"))
display(dbutils.fs.ls("dbfs:/user/hive/warehouse/formula1.db/circuit_test"))


# COMMAND ----------

df_test = spark.readStream.format("rate").option("rowsPerSecond", 1).load()

query = df_test.writeStream.format("delta") \
    .option("checkpointLocation", "dbfs:/tmp/checkpoints/test1") \
    .option("path", "dbfs:/tmp/test1") \
    .outputMode("append") \
    .start()


# COMMAND ----------

df_final.write.format("delta").mode("overwrite").saveAsTable("formula1.circuit")
    # .option("path", "dbfs:/user/hive/warehouse/formula1.db/circuit_test") \
    

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from hive_metastore.formula1.circuit;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC DROP TABLE IF EXISTS eclerx_emp;
# MAGIC
# MAGIC CREATE TABLE eclerx_emp_parq (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   dept STRING
# MAGIC )
# MAGIC USING parquet
# MAGIC
