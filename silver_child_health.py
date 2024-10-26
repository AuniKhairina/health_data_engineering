# Databricks notebook source
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import date
import pandas as pd
import numpy as np
import io
from pyspark.sql.functions import regexp_replace
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

# COMMAND ----------

storage_account_key = dbutils.secrets.get(scope="new-scope-cuz-of-hadi", key="auniadls-key")

# COMMAND ----------

# for databrick to access azure storage using the key
spark.conf.set("fs.azure.account.key.auniadls.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

# Read which file in bronze
df = spark.read.parquet((f"abfss://bronze@auniadls.dfs.core.windows.net/Child_Health_Observations"))

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

# To view column name & data type
df.printSchema()

# COMMAND ----------

df_filtered1 = df.filter("index > 0")

# COMMAND ----------

df_filtered1.limit(20).display()

# COMMAND ----------

# Need to remove special characters & white space in column "GHO (DISPLAY)""
# add new column with cleaned data
df_cleaned = df_filtered1.withColumn(
    "CLEANED_OBSERVATION",  # New column name
    regexp_replace(
        regexp_replace(
            regexp_replace(df_filtered1["GHO (DISPLAY)"], "[\\s-]", "_"),  # Replace spaces and hyphens with underscore
            "[()<>%]", ""  # Remove parentheses"()", less-than sign, and percentage sign
        ),
        "_+", "_"  # Handle multiple consecutive underscores
    )
)


# COMMAND ----------

# Convert 'value' column to Integer
df_cleaned = df_cleaned.withColumn("Numeric", col("Numeric").cast(IntegerType()))

# COMMAND ----------

df_cleaned.display()

# COMMAND ----------

df_renamed = df_cleaned.select(
    df_cleaned['CLEANED_OBSERVATION'].alias('OBSERVATION'),
    df_cleaned['YEAR (DISPLAY)'].alias('YEAR'),
    df_cleaned['Numeric'].alias('VALUE')
    )

# COMMAND ----------

df_renamed.display()

# COMMAND ----------

# partition data by observation
df_renamed.write.mode("overwrite").partitionBy("OBSERVATION").parquet("abfss://silver@auniadls.dfs.core.windows.net/Child_Health_Observations")
