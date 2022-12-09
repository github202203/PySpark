# Databricks notebook source
# MAGIC %fs ls /mnt/gold/Internal/DeltaLake/Claim/dbo_DimBroker/

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

df_d = spark.read.format("delta").load("/mnt/gold/Internal/DeltaLake/Claim/dbo_DimBroker/")
display(df_d)

# COMMAND ----------

check_dups = df_d.join(df_d.groupBy(df_d.columns).agg((f.count("*")>1).cast("int").alias("Duplicate_indicator")),on=df_d.columns,how="inner")
totalcounts = df_d.count()
print(totalcounts)
duplicate_count = check_dups.where(check_dups.Duplicate_indicator == 1).count()
print(duplicate_count)
if duplicate_count == 0:
    status="pass"
elif duplicate_count > 0:
    status="fail"
print(status)    

# COMMAND ----------

# MAGIC %fs ls /mnt/bronze/underwriting/Internal/Eclipse/DeltaLake/

# COMMAND ----------

df=spark.read.format("delta").load("/mnt/bronze/underwriting/Internal/Eclipse/DeltaLake/dbo_ApplicationUser_test/")
display(df)

# COMMAND ----------

df = spark.read.format("parquet").load("/mnt/silver/underwriting/Internal/Eclipse/Staging/dbo_ApplicationUser/")
display(df)

# COMMAND ----------

df = spark.read.format("parquet").load("/mnt/landing/underwriting/Internal/Eclipse/dbo_BusinessCode/")
display(df)

# COMMAND ----------

check_dups = df.join(df.groupBy(df.columns).agg((f.count("*")>1).cast("int").alias("Duplicate_indicator")),on=df.columns,how="inner")
totalcounts = df.count()
print(totalcounts)
duplicate_count = check_dups.where(check_dups.Duplicate_indicator == 1).count()
print(duplicate_count)
if duplicate_count == 0:
    status="pass"
elif duplicate_count > 0:
    status="fail"
print(status)    
