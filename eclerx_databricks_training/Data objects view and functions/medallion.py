# Databricks notebook source
Medallion Architecture or Multi hop Architecture or bronze silver gold Architecture 
************************************************************************************

Refining the data from left to right level


https://docs.databricks.com/aws/en/lakehouse/medallion

1. sources
******
kafka 
kinesis
csv,json,txt
data lake
spark
aws/gcs


2.ingestion transformation
Bronze(Raw ingestion and history)
*******
keep raw/hostory/spurce data in delta format with touching anything by ingestion from different sources 



transformaton layer on top of bronze layer data


Silver(validated)(filtered,cleaned,augmented)
******
consists of transformation but not aggregated and stored filtered data for last certian period of time
cleaned filered no duplicates data

aggregarion layer on top of silver layer data


Gold(enriched)(Business-level aggregates)
*****
mostly of the time we keep all the gold level layer in views but not mandatory



3.downstream
**********
Bi Reporting
streaming analytics
data science
data sharing




