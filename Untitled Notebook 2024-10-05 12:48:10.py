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

# MAGIC %sql
# MAGIC set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;
