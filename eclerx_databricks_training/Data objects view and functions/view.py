# Databricks notebook source
views:
    A virtual tables

    1. standard view (persisted :- saved to schema/catalog): can create by  SQL
    2. Temp View (valid till comute is on and not saved): can create by SQL and Pyspark
    

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/bikeSharing/data-001/
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %fs ls dbfs:/databricks-datasets/bikeSharing/data-001/
# MAGIC
# MAGIC views:
# MAGIC     A virtual tables
# MAGIC
# MAGIC     1. standard view (persisted :- saved to schema/catalog): can create by  SQL
# MAGIC     2. Temp View (valid till comute is on and not saved): can create by SQL and Pyspark
# MAGIC     
# MAGIC standared view
# MAGIC **************
# MAGIC create or replace view max_month as 
# MAGIC select mnth,round(max(temp),2) as max from bike_day group by mnth order by max desc;
# MAGIC
# MAGIC
# MAGIC this view will be persisted in under the schema
# MAGIC show views;
# MAGIC
# MAGIC select * from max_month;
# MAGIC
# MAGIC desc extended max_month;
# MAGIC     gives type as view and not a delta table coz its a virtual table and size is not assosiated for view coz evertime it takes the query and run it 
# MAGIC
# MAGIC
# MAGIC
# MAGIC temp view
# MAGIC *********
# MAGIC create or replace temp view holiday_count_temp as
# MAGIC select mnth, count(*) from bike_day where holiday = 1 group by mnth order by mnth 
# MAGIC
# MAGIC
# MAGIC this view wont be persisted and not shown iunder schema also and valid upto the spark session is on
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC functions(UDF)
# MAGIC **********
# MAGIC
# MAGIC
# MAGIC create or replace function funation_name(parameter dataypte)
# MAGIC returns datatype
# MAGIC return logic
# MAGIC
# MAGIC ex1:-
# MAGIC create or replace function fullname(first_name string, last_name string)
# MAGIC returns string
# MAGIC return concat(first_name," ", last_name);
# MAGIC
# MAGIC
# MAGIC select fullname("naval","yemul") as name
# MAGIC
# MAGIC ex2:-
# MAGIC create or replace function age_group(age int)
# MAGIC returns string
# MAGIC return case when age > 60 then 'senior' when age >=20 and age <=60 then 'adult' when age < 20 then 'teenager' else 'Kid' end ;
# MAGIC

# COMMAND ----------

df = spark.read.csv("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv",header=True,inferSchema=True)

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/bike_day/", True)

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation",True)

# COMMAND ----------

df.write.format("parquet").mode("overwrite").saveAsTable("bike_day")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 2: Now register it as external table
# MAGIC DROP TABLE IF EXISTS bike_day;
# MAGIC
# MAGIC CREATE TABLE bike_day
# MAGIC USING PARQUET
# MAGIC LOCATION 'dbfs:/FileStore/tables/bike_day/';
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace function fullname(first_name string, last_name string)
# MAGIC returns string
# MAGIC return concat(first_name, " ", last_name);
# MAGIC
# MAGIC
# MAGIC select fullname("naval","yemul") as name

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC create  table if not exists example (id int, forename string, surname string, age int) using parquet;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into example values(1,'Akash','yadav',28),(2,'Aman','Arora',28),(3,'Bhavesh','mahajan',28);
