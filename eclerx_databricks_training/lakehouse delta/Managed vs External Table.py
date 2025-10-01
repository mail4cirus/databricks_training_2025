# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Managed table
# MAGIC ************
# MAGIC below are both managed table and default delta format. location is used internally default loaction and saved the data and managed by databricks. default location :- dbfs:/user/hive/warehouse/table_name.
# MAGIC
# MAGIC   create table table_name (col1 datatype, col2 datatype)
# MAGIC
# MAGIC   or
# MAGIC
# MAGIC   df = spark.read.format("path")
# MAGIC   df.write.saveAsTable("table_name")
# MAGIC
# MAGIC
# MAGIC
# MAGIC External table
# MAGIC **************
# MAGIC external location(s3/gcs/adls/any other location) is given while creating the table. by using keyword location while creating the table.
# MAGIC
# MAGIC   create table table_name (col1 datatype, col2 datatype) location 'dbfs:/eclerx/metadata'
# MAGIC
# MAGIC   or
# MAGIC
# MAGIC   df = spark.read.format("path")
# MAGIC   df.write.option("path","dbfs:/eclerx/metadata").saveAsTable("table_name")
# MAGIC
# MAGIC
# MAGIC Difference is drop
# MAGIC   when we drop the managed table means drop everything i.e table + metadata(parquet + delta logs)
# MAGIC
# MAGIC
# MAGIC   when we drop the external table means just drop table metadata remains
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
