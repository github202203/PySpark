# Databricks notebook source
# MAGIC %fs mounts

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze/underwriting/Internal/Eclipse/Staging/dbo_Addr/SystemLoadID=1022022120601/")

# COMMAND ----------

df_s = spark.read.format("parquet").load("/mnt/bronze/underwriting/Internal/Eclipse/Staging/dbo_Addr/")
display(df_s)
df_s.count()

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze/underwriting/Internal/Eclipse/DeltaLake/")

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze/underwriting/Internal/Eclipse/DeltaLake/dbo_Addr/")

# COMMAND ----------

df = spark.read.format("delta").load("/mnt/bronze/underwriting/Internal/Eclipse/DeltaLake/dbo_Addr/")
display(df)
df.count()

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE HISTORY delta.`/data/dbo_Addr/`

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

df = spark.read.format("parquet").load("/mnt/bronze/underwriting/Internal/Eclipse/Staging/dbo_ClaimStatus/SystemLoadID=*/*.parquet")
display(df)

# COMMAND ----------

df = spark.read.format("delta").load("/mnt/bronze/underwriting/Internal/Eclipse/DeltaLake/dbo_ClaimStatus/")
display(df)
