# Databricks notebook source
Database: Transcational data happenning on daily basis stored in structured base with constraints with normaliazed form


Datawarehouse: Data stored for analtytic purpose in denormailaized form with most of the time structure data 

Data Lake: Store data with any file format structured, semi, unstructure ex: blob and ADLS(azure data lake storage), s3, gcs.


Lakehouse: datawarehouse + data lake
    It brings the best of these worlds
    It is a layer on top of your existing data lake 
        suppose if we have adls/s3/gc we can build lake house
            in order to make lake house there are few formats we have to follow
            if the storage bucket contains delta formats then we can call it as lake house

advance table formats 
like delta, iceberg, hudi helps you to build lakehouse  (bringing acid support)


Databricks : creators of table format called delta
    Delta lake : storage framework -- open source donated to linux foundation
    snowflake is using iceberg format to build delta lake
    But in databricks we can build delta lake using delta/iceberg/hudi formats
    But by default Databricks uses delta format. so by default all the tables are delta tables in any way the table has created (pyspark/sql)
    if we want to save dataframe as delta/iceberg table in other platform apart from databricks we have to use format as delta/iceberg like below
    df_final.write.mode("overwrite").format("delta").saveAsTable("table_name")
    df_final.write.mode("overwrite").format("iceberg").saveAsTable("table_name")

    delta lake website:- https://delta.io/





DataWarehouse               Data Lake               LakeHouse(Delta Lake)
***************************************************************************
Only structured data        structured/             structured/             
                            semi-strutured/         semi-strutured/
                            unstructured            unstructured/
                                                    Streaming


Scehma-on-write             Schema-on-read          schema-on-read
(first create a correct     (store the data 
 schema then load into      then future create 
 datawarehouse              schema then read it)
 )


Support Acid Transaction    minimal support to       Support Acid Transaction
                            Acid Transaction            in order to support acid property 
                            (might corrupt the data     using datalake they build 
                            while system failure)       lake House(deltalake )


Does not corrupt the        Leaves system in          Does not corrupt the system
system                      corrputed state

*******************************************************************************************

DataLake                LakeHouse                           Data warehouse
    open                one platform that unify all             Reliable
    Flexible            of your data engineering                Strong governance
    ML Support          analytics and AI workloads              Performance







Parquet: stores the data in columunar format also compressess the data using gzip snappy...
    
    ex:- suppose if we have 1 gb data csv file and simple etl convert csv into parquet format by saving into table. then the percentage conversion will be approximately upto 97% that comes around 17mb parquet file. so 70-97% conversion will happen based on the dataframe

Delta lake :- extension of parquet format with some additional features
Iceberg :- extension of parquet format with some additional features








# COMMAND ----------

parquet was not capable of evolving schema.
    if we want to add new column we have to alter the table add the column to parquet then need to insert the data
    if we update some records and we have to undo(trake previous version time travel) and it is not possible 
    ACID properties wont work 
