# Databricks notebook source
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import date
import pandas as pd
import numpy as np
import io

# COMMAND ----------

storage_account_key = dbutils.secrets.get(scope="new-scope-cuz-of-hadi", key="auniadls-key")

# COMMAND ----------

# for databrick to access azure storage using the key
spark.conf.set("fs.azure.account.key.auniadls.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

# read data from azure storage - auniadls/incoming/child-health-indicators-for-malaysia-3.csv
df = (spark.read
      .option("header","true")
      .option("inferSchema","true") 
      .option("delimiter",",")
      .csv("abfss://incoming@auniadls.dfs.core.windows.net/child-health-indicators-for-malaysia-3.csv")
      )

# COMMAND ----------

df.limit(20).display()

# COMMAND ----------

df.write.mode("overwrite").parquet(f"abfss://bronze@auniadls.dfs.core.windows.net/Child_Health_Observations")
