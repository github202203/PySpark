# Databricks notebook source
dbutils.widgets.removeAll()




# COMMAND ----------

dbutils.widgets.text("waitTime", "1" ,"WaitTime In Minutes")

# COMMAND ----------

wt = dbutils.widgets.get("waitTime")

# COMMAND ----------

# MAGIC 
# MAGIC %run ./fn_waitTimeMinutes_mb(wt)
