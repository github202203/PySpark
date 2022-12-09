# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Notebook
# MAGIC 
# MAGIC #### 
# MAGIC 
# MAGIC * **Title   :** Ingestion Delta
# MAGIC * **Description :** Ingest Staging (Source) Data to Delta Lake table (Target).
# MAGIC * **Language :** PySpark, SQL
# MAGIC 
# MAGIC #### History
# MAGIC | Date       | Developed By |Reason |
# MAGIC | :--------- | :----------  |:----- |
# MAGIC | 31/05/2022 | Raja Murugan | Ingest Staging (Source) Data to Delta Lake table (Target). |
# MAGIC | 27/06/2022 | Raja Murugan | Framework Improvement |
# MAGIC | 14/07/2022 | Anna Cummins | Refactor to increase Pyspark usage and reduce to single notebook for all x3 delta layer processes |
# MAGIC | 25/08/2022 | Anna Cummins | Handling for record deletions for incremental loads |

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Preprocess

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Include Common

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import desc, row_number, dense_rank, col, lit, explode
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
from delta.tables import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Reset Parameters

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Set Parameters

# COMMAND ----------

dbutils.widgets.text("DebugRun"           , "True"                                            , "DebugRun")
dbutils.widgets.text("SystemLoadID"       , "1032022061701"                                   , "SystemLoadID")
dbutils.widgets.text("SourcePath"         , "/bronze/underwriting/Internal/Eclipse/Staging"   , "SourcePath")
dbutils.widgets.text("TargetPath"         , "/bronze/underwriting/Internal/Eclipse/DeltaLake" , "TargetPath")
dbutils.widgets.text("TargetDatabaseName" , "EclipseBronze"                                   , "TargetDatabaseName")
dbutils.widgets.text("ObjectRunID"        , "1"                                               , "ObjectRunID")
dbutils.widgets.text("ObjectID"           , "1010001"                                         , "ObjectID")
dbutils.widgets.text("ObjectName"         , "dbo_Policy"                                      , "ObjectName")
dbutils.widgets.text("LoadType"           , "Incremental"                                     , "LoadType")
dbutils.widgets.text("UniqueColumn"       , "PolicyID"                                        , "UniqueColumn")
dbutils.widgets.text("PartitionColumn"    , ""                                                , "PartitionColumn")
dbutils.widgets.text("WatermarkValue"     , "1022022061701"                                   , "WatermarkValue")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Set Variables

# COMMAND ----------

# Parameter Variables
bol_debugRun           = eval(dbutils.widgets.get("DebugRun"))
int_systemLoadID       = int(dbutils.widgets.get("SystemLoadID"))
str_sourcePath         = dbutils.widgets.get("SourcePath")
str_targetPath         = dbutils.widgets.get("TargetPath")
str_targetDatabaseName = dbutils.widgets.get("TargetDatabaseName")
int_objectRunID        = int(dbutils.widgets.get("ObjectRunID"))
int_objectID           = int(dbutils.widgets.get("ObjectID"))
str_objectName         = dbutils.widgets.get("ObjectName")
str_loadType           = dbutils.widgets.get("LoadType")
str_uniqueColumn       = dbutils.widgets.get("UniqueColumn")
str_partitionColumn    = dbutils.widgets.get("PartitionColumn")
int_watermarkValue     = int(dbutils.widgets.get("WatermarkValue"))

# Process Variables
str_mountPath          = "/mnt"
str_sourceFilePath     = f"{str_mountPath}{str_sourcePath}/{str_objectName}"
str_targetTablePath    = f"{str_mountPath}{str_targetPath}/{str_objectName}"
str_targetTableName    = str_objectName
str_sourceViewName     = f"vw_{str_objectName}"
str_mergeViewName      = f"vw_{str_objectName}_merge"
str_skColumn           = f"SK_"+str_targetTableName+"_ID"
dt_utcNow              = datetime.utcnow()
str_load               = "NotStarted"
str_layer              = (str_targetPath.split("/")[1]).split("-")[-1].title() 

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Process

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Load latest data

# COMMAND ----------

try:
    df = (spark
          .read
          .option("mergeSchema","true") 
          .parquet(str_sourceFilePath) 
          .filter(col("SystemLoadID") > int_watermarkValue)
         )
except: 
    # for JSON file types we need to explode and select the data
    df = (spark
          .read
          .option("mergeSchema","true") 
          .json(str_sourceFilePath) 
          .filter(col("SystemLoadID") > int_watermarkValue)
          .withColumn("data", explode("data"))
          .select("data.*", "SystemLoadID")
         )

# Load the latest data and add SCD type 2 cols
df = (df.select("*",
                lit(int_systemLoadID).alias(str_layer+"SystemLoadID"),
                lit(True).alias("Current"),
                lit(dt_utcNow).alias("EffectiveDateUTC"),
                lit(None).cast(TimestampType()).alias("EndDateUTC")
               )
     )

# get the latest data per unique column/system load ID
arr_uniqueColumn  = [c.strip() for c in str_uniqueColumn.split(",")]

if str_uniqueColumn:
    window = Window.partitionBy(arr_uniqueColumn).orderBy(desc("SystemLoadID"))
    df = df.withColumn("RowNumber", row_number().over(window))
else:
    window = Window.partitionBy().orderBy(desc("SystemLoadID"))
    df = df.withColumn("RowNumber", dense_rank().over(window))

df = (df
      .filter(col("RowNumber") == 1)
      .withColumnRenamed("SystemLoadID", str_layer+"StagingSystemLoadID")
     )

# COMMAND ----------

if bol_debugRun:
  display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Initial Load

# COMMAND ----------

# Check if Delta Lake table exists
bol_dltExists = DeltaTable.isDeltaTable(spark, str_targetTablePath)

if(bol_dltExists == False):
# Create Database if not exist, for Delta Lake tables
    spark.sql(f"""
    CREATE DATABASE IF NOT EXISTS {str_targetDatabaseName}  
    COMMENT '{str_layer} database {str_targetDatabaseName}'
  """).display()

# Drop table from hive store if exists
    spark.sql(f"""
       DROP TABLE IF EXISTS {str_targetDatabaseName}.{str_targetTableName}
    """).display()
    
    if(str_targetDatabaseName != 'UnderwritingGold'):        
                
        # Write the table to ADLS and save in database
        (df
         .write
         .format("delta")
         .option("path",str_targetTablePath)
         .option("comment", lit(f"{str_layer} table {str_targetTableName}")) \
         .saveAsTable(f"{str_targetDatabaseName}.{str_targetTableName}")
        )
        
    else:
        
        # Added for Gold 
        spark.sql(f"""CREATE OR REPLACE TABLE {str_targetDatabaseName}.{str_targetTableName} (    
                         {str_skColumn} BIGINT GENERATED ALWAYS AS IDENTITY   
                )
        LOCATION '{str_targetTablePath}' """)
        
        (df
         .write
         .format("delta") \
         .mode("append") \
         .option("mergeSchema","true")\
         .save(str_targetTablePath)
        )
    
    str_load = "Initial"
    print("Initial Load Completed")

# COMMAND ----------

if bol_debugRun and str_load == 'Initial':
  spark.sql(f"DESCRIBE DATABASE {str_targetDatabaseName}").display()
  spark.sql(f"DESCRIBE TABLE {str_targetDatabaseName}.{str_targetTableName}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Full Load

# COMMAND ----------

if str_load == "NotStarted" and str_loadType == "FullRefresh":
  
  (df
   .write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", True)
   .option("path",str_targetTablePath)
   .saveAsTable(f"{str_targetDatabaseName}.{str_targetTableName}")
  )
  
  str_load = "Full"
  print("Full Load Completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Incremental Load

# COMMAND ----------

if str_load == "NotStarted" and str_loadType == 'Incremental':
  
  dlt_base = DeltaTable.forPath(spark, str_targetTablePath)
  
  str_mergeCondition = " AND ".join([ "Base." + str + " = Updates." + str for str in arr_uniqueColumn])
  print(f"str_mergeCondition: {str_mergeCondition}")  
  
  # Now upsert into the delta table
  (dlt_base.alias("Base")
   .merge(df.alias("Updates"), str_mergeCondition)
   .whenMatchedDelete(condition = "Updates.SYS_CHANGE_OPERATION = 'D'") ## CHECK: Is this generalised enough? What about future incremental sources, will we always have col of naming SYS_CHANGE_OPERATION?
   .whenMatchedUpdateAll()
   .whenNotMatchedInsertAll(condition = "Updates.SYS_CHANGE_OPERATION != 'D'")
   .execute()
  )
  
  str_load = "Merge"  
  print("Merge Load Completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6 Check Load

# COMMAND ----------

if bol_debugRun:
    df_updated = DeltaTable.forPath(spark, str_targetTablePath).toDF()
    display(df_updated.filter(col(str_layer+"SystemLoadID") == int_systemLoadID))

# COMMAND ----------

str_layer ='underwritingclaim_DimClaimStatus_SK'

# COMMAND ----------

print((str_layer.split("/")[1]).split("-")[-1].title() )

# COMMAND ----------

str_targetPath='underwritingclaim_DimClaimStatus'

# COMMAND ----------

print(str_targetPath.split("_")[1])

# COMMAND ----------

print("SK_"+str_targetPath.split("_")[1]+"ID")

# COMMAND ----------

