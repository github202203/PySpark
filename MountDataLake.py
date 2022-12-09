# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Notebook
# MAGIC 
# MAGIC #### 
# MAGIC 
# MAGIC * **Title   :** MountDataLake
# MAGIC * **Description :** Mount Azure Data Lake Storage Gen2.
# MAGIC * **Language :** PySpark
# MAGIC 
# MAGIC #### History
# MAGIC | Date       | Developed By |Reason |
# MAGIC | :--------- | :----------  |:----- |
# MAGIC | 31/05/2022 | Raja Murugan | Mount Azure Data Lake Storage Gen2 |

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Preprocess

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Include Common

# COMMAND ----------

# MAGIC %run ../Common/fn_mountDataLake

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Reset Parameters

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Set Parameters

# COMMAND ----------

dbutils.widgets.text("ContainerName", "bronze", "ContainerName")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Set Variables

# COMMAND ----------

# Parameter Variables
str_containerName = dbutils.widgets.get("ContainerName")

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Process

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Mount Data Lake

# COMMAND ----------

fn_mountDataLake(str_containerName)