# Databricks notebook source
# MAGIC %md
# MAGIC 1. BCG Immunization Coverage Among 1 year old child
# MAGIC 2. Hepatitis B Immunization Coverage Among 1 year old child
# MAGIC 3. DTaP Immunization Coverage Among 1 year old child
# MAGIC

# COMMAND ----------

from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from datetime import date
import pandas as pd
import numpy as np
import io
import matplotlib.pyplot as plt
import seaborn as sns


# COMMAND ----------

storage_account_key = dbutils.secrets.get(scope="new-scope-cuz-of-hadi", key="auniadls-key")

# COMMAND ----------

# for databrick to access azure storage using the key
spark.conf.set("fs.azure.account.key.auniadls.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

# Read all file in silver
df = spark.read.parquet((f"abfss://silver@auniadls.dfs.core.windows.net/Child_Health_Observations/"))

# COMMAND ----------

# MAGIC %md
# MAGIC Below are findings on which immunization increase and decrease among 1 year old child in Malaysia over the year.

# COMMAND ----------

# Read BCG Immunization file from silver
df_BCG_Immunization = spark.read.parquet((f"abfss://silver@auniadls.dfs.core.windows.net/Child_Health_Observations/OBSERVATION=BCG_immunization_coverage_among_1_year_olds_"))

# COMMAND ----------

df_BCG_Immunization.orderBy("YEAR").display()

# COMMAND ----------

# Read HepB Immunization file from silver
df_HepB_Immunization = spark.read.parquet((f"abfss://silver@auniadls.dfs.core.windows.net/Child_Health_Observations/OBSERVATION=Hepatitis_B_HepB3_immunization_coverage_among_1_year_olds_"))

# COMMAND ----------

# Read DTaP Immunization file from silver
df_DTaP_Immunization = spark.read.parquet((f"abfss://silver@auniadls.dfs.core.windows.net/Child_Health_Observations/OBSERVATION=Diphtheria_tetanus_toxoid_and_pertussis_DTP3_immunization_coverage_among_1_year_olds_"))

# COMMAND ----------

df_BCG_Immunization = df_BCG_Immunization.withColumn("Immunization_Type", lit("BCG"))
df_HepB_Immunization = df_HepB_Immunization.withColumn("Immunization_Type", lit("HepB"))
df_DTaP_Immunization = df_DTaP_Immunization.withColumn("Immunization_Type", lit("DTaP"))

df_combine_immune = df_BCG_Immunization.union(df_HepB_Immunization).union(df_DTaP_Immunization)

# COMMAND ----------

df_combine_immune.orderBy("Immunization_Type", "YEAR").display()

# COMMAND ----------

# Filter the DataFrame to include only rows where the 'year' is between 2000 and 2018
filtered_df = df_combine_immune.filter((col("year") >= 2010) & (col("year") <= 2018))

# COMMAND ----------

filtered_df.write.mode("overwrite").parquet(f"abfss://gold@auniadls.dfs.core.windows.net/Child_Health_Observations_Immunization")

# COMMAND ----------

df1 = filtered_df

# COMMAND ----------

# Convert Spark DataFrame to Pandas DataFrame
df1_pandas = df1.toPandas()

# Set the aesthetic style of the plots
sns.set(style="whitegrid")

# Create a line plot
plt.figure(figsize=(10, 6))

# Use seaborn's lineplot to plot 'year' on x-axis and 'value' on y-axis, grouping by 'Immunization_Type'
sns.lineplot(
    x='YEAR', 
    y='VALUE', 
    hue='Immunization_Type', 
    data=df1_pandas,
    marker = 'o'
)

# Adding labels and title
plt.title('Vaccine Value Over Years', fontsize=12)
plt.xlabel('Year', fontsize=10)
plt.ylabel('Value', fontsize=10)

# Display the plot
plt.show()

# COMMAND ----------


