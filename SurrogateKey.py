# Databricks notebook source
from delta.tables import *

# COMMAND ----------

spark.sql("""
        CREATE OR REPLACE TABLE demo (          
          product_type STRING,
          sales BIGINT
)""")

# COMMAND ----------

spark.sql("""
        CREATE OR REPLACE TABLE demo (
          id BIGINT GENERATED ALWAYS AS IDENTITY
)""")

# COMMAND ----------

DeltaTable.createOrReplace(spark)\
    .addColumn("id", "BIGINT", "NOT NULL GENERATED ALWAYS AS IDENTITY")\
    .location("/tmp/default/demo") \
    .execute()

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
  .tableName("default.people10m") \
  .addColumn("id", "INT") \
  .addColumn("firstName", "STRING") \
  .addColumn("middleName", "STRING") \
  .addColumn("lastName", "STRING", comment = "surname") \
  .addColumn("gender", "STRING") \
  .addColumn("birthDate", "TIMESTAMP") \
  .addColumn("ssn", "STRING") \
  .addColumn("salary", "INT") \
  .execute()

# COMMAND ----------

str_targetDatabaseName = "underwritinggold"
str_targetTableName = "underwritingclaim_dimclaimstatus_SK"
str_skColumn = "SK_"+str_targetTableName+"_Id"

# COMMAND ----------

"SK_"+{str_targetTableName}+"_Id"

# COMMAND ----------

 spark.sql(f""" CREATE OR REPLACE TABLE {str_targetDatabaseName}.{str_targetTableName} ({str_skColumn}  BIGINT GENERATED ALWAYS AS IDENTITY
       )""").display()

# COMMAND ----------

