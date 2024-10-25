# Databricks notebook source
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import date
import pandas as pd
import numpy as np
import io

# COMMAND ----------

storage_account_key = dbutils.secrets.get(scope="kotak-sakti-scope-111", key="accesskey-adls-adlskotaksakti1")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.adlskotaksakti1.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

# To Read data from ADLS
df = (spark.read
      .option("header","true")
      .option("inferSchema","true") #hope spark can understand the datatype of each column
      .option("delimiter","\t")
      .csv("abfss://incoming@adlskotaksakti1.dfs.core.windows.net/marketing_campaign.csv")
      )

# COMMAND ----------

df.limit(5).toPandas()

# COMMAND ----------

# Connect to my own datalake
storage_account_key = dbutils.secrets.get(scope="kotak-sakti-scope-111", key="auniadls-key")


# COMMAND ----------

# A configuration to access aunidatalake = auniadls 
spark.conf.set("fs.azure.account.key.auniadls.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

today = date.today()

# COMMAND ----------

output_folder_path = f"abfss://bronze@auniadls.dfs.core.windows.net/marketing_campaign/marketing_campaign_output_bronze_{today}"


# COMMAND ----------

output_folder_path

# COMMAND ----------

# overwrite : the new file will always the priovous file, data previous akan hilang. melainkan, kau tukar nama file baru
#df.write.mode("append").parquet("abfss://bronze@auniadls.dfs.core.windows.net/marketing_campaign_output_bronze")
# write the parquet file in my BRONZE folder
df.write.mode("overwrite").parquet(output_folder_path)

# COMMAND ----------


