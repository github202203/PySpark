# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Function
# MAGIC 
# MAGIC ####  
# MAGIC * **Title   :** fn_waitTimeMinutes
# MAGIC * **Description :** Waits for 'N' Time Minutes based on the value passed.
# MAGIC * **Language :** PySpark
# MAGIC 
# MAGIC #### History
# MAGIC | Date       | Developed By |Reason |
# MAGIC | :--------- | :----------  |:----- |
# MAGIC | 19/10/2022 | Anna Cummins | Waits for 'N' Time Minutes based on the value passed |

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Preprocess

# COMMAND ----------

import sys
import time

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 1.2 Set Parameters

# COMMAND ----------

dbutils.widgets.text("waitTimeMinutes", "1" ,"WaitTime In Minutes")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 1.3 Set Variables

# COMMAND ----------

int_waitTimeMinutes =  int(dbutils.widgets.get("waitTimeMinutes"))

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Sleep for N' Minutes

# COMMAND ----------

def fn_waitTimeMinutes(x: int):
    time.sleep(x*60)
    
fn_waitTimeMinutes(int_waitTimeMinutes)
