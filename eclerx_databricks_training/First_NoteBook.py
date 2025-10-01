# Databricks notebook source
print("testing")

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC println("vishnu")

# COMMAND ----------

# MAGIC %sql
# MAGIC select "Run SQL"

# COMMAND ----------

dbutils.fs.rm('dbfs:/user/hive/warehouse/demo.db', recurse=True)




# COMMAND ----------

dbutils.fs.rm('dbfs:/user/hive/warehouse/formula1.db', recurse=True)

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS demo;
# MAGIC drop table IF EXISTS demo.emp;
# MAGIC CREATE TABLE IF NOT EXISTS demo.emp (
# MAGIC     id int ,
# MAGIC     name string ,
# MAGIC     salary float
# MAGIC );
# MAGIC
# MAGIC INSERT INTO demo.emp (id, name, salary)
# MAGIC VALUES
# MAGIC     (101, "vishnu",29735.0),
# MAGIC     (102, "varun",30000.0),
# MAGIC     (103, "vishwa",2893.0),
# MAGIC     (104, "preethi",29888.0),
# MAGIC     (105, "anjali",304928.0),
# MAGIC     (106, "deepa",12000.0),
# MAGIC     (107, "prakash",23847.0),
# MAGIC     (108, "sunil",29389),
# MAGIC     (109, "rajath",29735)
# MAGIC     ;
# MAGIC
# MAGIC select * from demo.emp;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show schemas;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe hive_metastore.demo.emp;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC describe extended hive_metastore.demo.emp;

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/
# MAGIC

# COMMAND ----------

#creating new folder in DBFS using dbutils

dbutils.fs.mkdirs("dbfs:/FileStore/eclerx_input_files")

# COMMAND ----------

#copying file from one folder to another using dbutils

dbutils.fs.cp("dbfs:/FileStore/tables/drivers.json","dbfs:/FileStore/eclerx_input_files/")



# COMMAND ----------


