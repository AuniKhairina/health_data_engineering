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

spark.conf.set("fs.azure.account.key.auniadls.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

df = spark.read.csv(f"abfss://bronze@auniadls.dfs.core.windows.net/Child_Health_Observations")
