# Databricks notebook source
# %sh
# ls /mnt/
# dbutils.fs.ls("abfss://pub-env@stpdlincaesa.dfs.core.windows.net/")

dbutils.fs.ls("abfss://dev-sandbox-syn@stnsycaesa.dfs.core.windows.net/CrisHudici")

# COMMAND ----------

df = spark.read.option("header",True).csv("abfss://dev-sandbox-syn@stnsycaesa.dfs.core.windows.net/CrisHudici/toronto_departures.csv")
display(df)

# COMMAND ----------

# import pyreadstat
# import saspy
import pandas as pd
# import sas7bdat
# from sas7bdat import *
import platform
import pyspark
print('Python: ', platform.python_version())
print('pandas: ', pd.__version__)

#https://stackoverflow.com/questions/69293491/file-found-on-pyspark-but-not-found-in-pandas
# Pandas only read from local file system and that's the reason why it cannot find the file.
# sasFile = '/dbfs/mnt/pub-env/CrisHudici/class.sas7bdat'
# sasFile = '/dbfs/mnt/pub-env/CrisHudici/airline.sas7bdat'
sasFile = '/dbfs/mnt/pub-env/CrisHudici/airline.sas7bdat'
# https://stackoverflow.com/questions/49059421/pandas-fails-with-correct-data-type-while-reading-a-sas-file
# sasFile = 'file:/mnt/pub-env/CrisHudici/airline.sas7bdat'
# foo = SAS7BDAT(sasFile)
# with SAS7BDAT(sasFile, skip_header=False) as reader:
#  df = reader.to_data_frame()
df = pd.read_sas(sasFile, format='sas7bdat', index=None, encoding=None, chunksize=None, iterator=False)
df.head(5)
#df = spark.read.format('dat').options(header='true', inferSchema='false').dat(sasFile) 
#df = df.head()
#print(df)
#type(df)


# COMMAND ----------

textFile = '/mnt/pub-env/CrisHudici/shakespeare - short.txt'
df = spark.read.text(textFile)
df.head(10)

# COMMAND ----------

# MAGIC %r
# MAGIC # install.packages("haven")
# MAGIC library(haven)
# MAGIC sasFile = '/mnt/pub-env/CrisHudici/airline.sas7bdat'
# MAGIC srcTaxFfs = read_sas(sasFile)

# COMMAND ----------

# MAGIC %r
# MAGIC # install.packages("haven")
# MAGIC library(haven)
# MAGIC # install.packages("sas7bdat")
# MAGIC # install.packages("pyreadstat")
# MAGIC # install.packages("xport")
# MAGIC # library(sas7bdat)
# MAGIC # path = '/mnt/pub-env/CrisHudici'
# MAGIC # sasFile = path + 'airline.sas7bdat'
# MAGIC sasFile <- '/dbfs/mnt/pub-env/CrisHudici/airline.sas7bdat'
# MAGIC # df <- read_sas("airline.sas7bdat")
# MAGIC df <- read_sas(sasFile)
# MAGIC head(df)
# MAGIC # df <- read.sas7bdat(sasFile, NULL)
# MAGIC # write_sas(df, '/dbfs/mnt/pub-env/CrisHudici/sasfile.sas7bdat')
# MAGIC # head(df, 6)
# MAGIC # View(df)
# MAGIC # df <- read_sas("sasfile.sas7bdat")
# MAGIC # View(df)

# COMMAND ----------

# MAGIC %python
# MAGIC import os
# MAGIC import zipfile
# MAGIC os.chdir('/dbfs/mnt/pub-env/CrisHudici/')
# MAGIC cwd = os.getcwd()
# MAGIC print("The current working directory: {0}".format(cwd))
# MAGIC os.listdir('./')
# MAGIC sasFile = '/dbfs/mnt/pub-env/CrisHudici/class.sas7bdat'
# MAGIC df = pd.read_sas(sasFile, format='sas7bdat', index=None, encoding=None, chunksize=None, iterator=False)
# MAGIC df.head(10)

# COMMAND ----------

import pandas as pd
import platform
import pyspark

# Reads a SAS file, converts it to parquet and csv and displays the first 5 rows
path = '/dbfs/mnt/pub-env/CrisHudici'
sasFile = path + '/airline.sas7bdat'
df = pd.read_sas(sasFile, format='sas7bdat', index=None, encoding=None, chunksize=None, iterator=False)
parquetFile = path + './airline.parquet'
df.to_parquet(parquetFile)
csvFile = path + './airline.csv'
df.to_csv(csvFile)
df.head(5)

# COMMAND ----------

import pyspark

# Departures from Toronto Pearson International Airport (YYZ): 03/05/2023
path = '/mnt/pub-env/CrisHudici'
csvFile = path + '/toronto_departures.csv'
df = spark.read.csv(csvFile, header='true', inferSchema='true')
df.createOrReplaceTempView('toronto_departures')     # saved in a hadoop table (view) - see Data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Destination, Airline, Status, count(*) as Num_flights
# MAGIC FROM toronto_departures
# MAGIC --WHERE Status = 'Delayed'
# MAGIC Group BY Destination, Airline, Status
# MAGIC ORDER BY Num_flights DESC 

# COMMAND ----------

import pandas as pd
import pyspark
# https://stackoverflow.com/questions/51949414/read-sas-sas7bdat-data-with-spark
# https://spark-packages.org/package/saurfang/spark-sas7bdat
path = '/dbfs/mnt/pub-env/CrisHudici'
sasFile = path + '/airline.sas7bdat'
df = pd.read_sas(sasFile, format='sas7bdat', index=None, encoding=None, chunksize=None, iterator=False)
# csvFile = path + './airline.csv'
# df.to_csv(csvFile)
df.head(3)
df.createOrReplaceTempView('airline')

# COMMAND ----------

from pyspark.sql import SQLContext

path = '/dbfs/mnt/pub-env/CrisHudici'
sasFile = path + '/airline.sas7bdat'
sqlContext = SQLContext(sc)
# df = sqlContext.read.format("com.github.saurfang.sas.spark").load(sasFile)
# df = sqlContext.read.format("https://repos.spark-packages.org/").load(sasFile)
df = sqlContext.read.format("https://spark.apache.org/third-party-projects.html").load(sasFile)
df.createOrReplaceTempView('airline')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM airline
# MAGIC LIMIT 3

# COMMAND ----------

import pandas as pd
import platform
import pyspark

# Reads a SAS file, converts it to parquet and csv and displays the first 3 rows
path = '/dbfs/mnt/pub-env/CrisHudici'
sasFile = path + '/airline.sas7bdat'
df = pd.read_sas(sasFile, format='sas7bdat', index=None, encoding=None, chunksize=None, iterator=False)
parquetFile = path + './airline.parquet'
df.to_parquet(parquetFile)
csvFile = path + './airline.csv'
df.to_csv(csvFile)
df.head(3)

# COMMAND ----------

import pyspark

path = '/mnt/pub-env/CrisHudici'
csvFile = path + '/airline.csv'
df = spark.read.csv(csvFile, header='true', inferSchema='true')
df.createOrReplaceTempView('airline')

# COMMAND ----------

read_format = 'csv'
write_format = 'delta'
load_path = 'abfss://pie-eip-p@stpdmfdiiun01sa.dfs.core.windows.net/CrisHudici/toronto_departures.csv'
save_path = 'abfss://pie-eip-p@stpdmfdiiun01sa.dfs.core.windows.net/CrisHudici/'
table_name = 'departures'

spark.sql("CREATE TABLE " + table_name + " USING CSV LOCATION '" + load_path + "'") 
display(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM departures
# MAGIC LIMIT 3

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS tor_dep2;
# MAGIC CREATE TABLE IF NOT EXISTS tor_dep2 USING CSV LOCATION 'abfss://pie-eip-p@stpdmfdiiun01sa.dfs.core.windows.net/CrisHudici/toronto_departures.csv';
# MAGIC SELECT *
# MAGIC FROM tor_dep2
# MAGIC LIMIT 4

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM departures
# MAGIC LIMIT 3