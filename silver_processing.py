# Databricks notebook source
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import date
import pandas as pd
import numpy as np
import io

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('new-scope-cuz-of-hadi')

# COMMAND ----------

storage_account_key = dbutils.secrets.get(scope="kotak-sakti-scope-111", key="auniadls-key")


# COMMAND ----------

spark.conf.set("fs.azure.account.key.auniadls-key.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

today = date.today()

# COMMAND ----------

filename = f"marketing_campaign_output_bronze_{today}"

# COMMAND ----------

filename

# COMMAND ----------

# To read file from BRONZE/marketing_campaign
df = spark.read.parquet(f"abfss://bronze@auniadls.dfs.core.windows.net/marketing_campaign/{filename}")

# COMMAND ----------

df.limit(5).toPandas()

# COMMAND ----------

# Do some cleaning data here

# COMMAND ----------

# create path to READ data into what folder (only read, not write yet)
output_folder_path = f"abfss://silver@auniadls.dfs.core.windows.net/marketing_campaign/marketing_campaign_silver_output_{today}"


# COMMAND ----------

df.write.mode("append").parquet(output_folder_path)

