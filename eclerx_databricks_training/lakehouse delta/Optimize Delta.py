# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC create table demo (id int, name string);
# MAGIC
# MAGIC insert into demo values(1,'a');
# MAGIC insert into demo values(2,'b');
# MAGIC insert into demo values(3,'c');
# MAGIC insert into demo values(4,'d');
# MAGIC insert into demo values(5,'k');
# MAGIC insert into demo values(6,'u');
# MAGIC insert into demo values(7,'i');
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC Z - Order 
# MAGIC   using indexing for columns so that it will store data in order wise based on the columns
# MAGIC
# MAGIC   optimize table_name
# MAGIC   zorder by (column_name)
# MAGIC
# MAGIC   disadvantages
# MAGIC   we can order by column1 but analytics team are using different column for filtering then this will not an effictive. in order to overcome this liquid clustering technique is there
# MAGIC
# MAGIC Liquid clustering
# MAGIC   no need to mention any columns. based on the frequent filtering columns automatically it will decide and do the order store
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC delete from table_name
# MAGIC   all the records are dropped 
# MAGIC
# MAGIC   if we want back data/table we can use cte and version from the time travel 
# MAGIC   But in delta restore options is there where we can get history table with the below command
# MAGIC   
# MAGIC   i.e... restore table table_name to version as of version_number
# MAGIC
# MAGIC
# MAGIC
