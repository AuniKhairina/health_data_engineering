# Databricks notebook source
# MAGIC %md
# MAGIC - Extract health care data from Kaggle
# MAGIC - Dump data into Azure Data Lake Storage
# MAGIC - Read data from ADLS into Bronze Notebook
# MAGIC - Data Cleaning
# MAGIC - Write csv file in Bronze folder ADLS
# MAGIC - Read data from Bronze into Silver Notebook
# MAGIC - Separate by Gold 1, Gold 2 notebook
# MAGIC - Start data pipeline using Airflow (connect notebook to Airflow)
# MAGIC - Create Power BI dashboards based on Gold 1, Gold 2
# MAGIC
# MAGIC

# COMMAND ----------

from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import date
import pandas as pd
import numpy as np
import io
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# COMMAND ----------

storage_account_key = dbutils.secrets.get(scope="new-scope-cuz-of-hadi", key="auniadls-key")

# COMMAND ----------

# for databrick to access azure storage using the key
spark.conf.set("fs.azure.account.key.auniadls.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

# read data from azure storage - auniadls/incoming/child-health-indicators-for-malaysia-3.csv
df = (spark.read
      .option("header","true")
      .option("inferSchema","true") # hope spark can understand the datatype of each column
      .option("delimiter",",")
      .csv("abfss://incoming@auniadls.dfs.core.windows.net/child-health-indicators-for-malaysia-3.csv")
      )

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

# To view column name & data type
df.printSchema()

# COMMAND ----------

# 1) Assigning a row number to each row
# 2) Filtering out the first row
# 3) Dropping the row 1

# Define a row number window
window_spec = Window.orderBy(F.lit(1))  # This assigns a row number for all rows, lit(1) orders arbitrarily


# COMMAND ----------

# Add a row number column
df_with_rownum = df.withColumn("row_num", F.row_number().over(window_spec))

# COMMAND ----------

# Filter out the first row
df_1 = df_with_rownum.filter(F.col("row_num") > 1).drop("row_num")

# COMMAND ----------


# Show the result, after filtering out the first row
df_1.limit(5).display()

# COMMAND ----------

df_renamed = df_1.select(
    df_1['GHO (DISPLAY)'].alias('OBSERVATION'),
    df_1['GHO (URL)'].alias('OBSERVATION(URL)'),
    df_1['YEAR (DISPLAY)'].alias('YEAR'),
    df_1['COUNTRY (DISPLAY)'].alias('COUNTRY'),
    df_1['SEX (DISPLAY)'].alias('GENDER'),
    df_1['Numeric'].alias('VALUE'),
    df_1['Low'].alias('MIN VALUE'),
    df_1['High'].alias('MAX VALUE')
    )

# COMMAND ----------

df_renamed.display()

# COMMAND ----------

# partition data by year
df_renamed.write.mode("overwrite").partitionBy("OBSERVATION").csv("abfss://bronze@auniadls.dfs.core.windows.net/Child_Health_Observations")
