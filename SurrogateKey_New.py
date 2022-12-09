# Databricks notebook source
from delta.tables import *

# COMMAND ----------

spark.sql("""DROP TABLE IF EXISTS underwritinggold.underwriterclaim_DimClaimStatus_SK""" )

# COMMAND ----------

spark.sql("""CREATE OR REPLACE TABLE underwritinggold.underwriterclaim_DimClaimStatus_SK (    
          SK_DimClaimStatus_SK_ID BIGINT GENERATED ALWAYS AS IDENTITY        
          
)
LOCATION '/mnt/gold/Internal/DeltaLake/Claim/underwritingclaim_DimClaimStatus_SK' """)

# COMMAND ----------

spark.sql("""CREATE OR REPLACE TABLE underwritinggold.underwriterclaim_DimClaimStatus_SK (    
          SK_DimClaimStatus_SK_ID BIGINT GENERATED ALWAYS AS IDENTITY,         
          BK_ClaimStatusID INT,
          FK_BK_SourceSystemCode STRING,            
          ClaimStatus STRING,
          GoldStagingSystemLoadID BIGINT,
          GoldSystemLoadID BIGINT,
          Current BOOLEAN,
          EffectiveDateUTC timestamp,
          RowNumber INT
)
LOCATION '/mnt/gold/Internal/DeltaLake/Claim/underwritingclaim_DimClaimStatus_SK' """)

# COMMAND ----------



# COMMAND ----------

df_dataFrame \
    .write.format("delta") \
    .mode("append") \
    .save(str_deltaFilePath)