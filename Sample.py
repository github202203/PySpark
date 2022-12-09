# Databricks notebook source
# MAGIC %fs mounts

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets

# COMMAND ----------

# MAGIC %fs ls /mnt/bronze/

# COMMAND ----------

df_crs_causecode = spark.read.format("parquet").load("/mnt/bronze/adhoc/Internal/MyMIAdhocSource/Staging/dbo_CRS_CauseCodes/SystemLoadID=1072022100702/")
display(df_crs_causecode)
df_crs_causecode.count()

# COMMAND ----------

df_crs_causecodeall = spark.read.format("parquet").load("/mnt/bronze/adhoc/Internal/MyMIAdhocSource/Staging/dbo_CRS_CauseCodes/")

df_crs_causecodeall.count()

# COMMAND ----------

# MAGIC %fs ls /mnt/gold/Internal/Staging/Claim/

# COMMAND ----------

df_dimclaim = spark.read.format("parquet").load("/mnt/gold/Internal/Staging/Claim/underwritingclaim_DimClaim/")
display(df_dimclaim)
df_dimclaim.count()

# COMMAND ----------

df_dimclaimstatus = spark.read.format("parquet").load("/mnt/gold/Internal/Staging/Claim/underwritingclaim_DimClaimStatus/")
display(df_dimclaimstatus)
df_dimclaimstatus.count()

# COMMAND ----------

# MAGIC %fs ls /mnt/silver/underwriting/Internal/Eclipse/Staging/

# COMMAND ----------

# MAGIC %fs ls /mnt/silver/underwriting/Internal/Eclipse/Staging/dbo_BusinessCode/

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("mbtest").getOrCreate()

for table in spark.catalog.listTables():
	print(table.name)

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("mbtest").getOrCreate()

for database in spark.catalog.listDatabases():
	print(database.name)
    

# COMMAND ----------

for database in spark.catalog.listDatabases():
#     print(database)
    for table in spark.catalog.listTables(database.name):
#         print(table)
        for column in spark.catalog.listColumns(table.name,database.name):
            print(column)

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;
# MAGIC --desc demo

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from eclipsebronzetableslist

# COMMAND ----------

def flatten(S):
      if S == []:
        return S
      if isinstance(S[0], list):
        return flatten(S[0]) + flatten(S[1:])
      return S[:1] + flatten(S[1:])

# list of databases
db_list = [x[0] for x in spark.sql("SHOW DATABASES").rdd.collect()]

for i in db_list:
      spark.sql("SHOW TABLES IN {}".format(i)).createOrReplaceTempView(str(i)+"TablesList")

# create a query for fetching all tables from all databases
union_string = "SELECT database, tableName FROM "
for idx, item in enumerate(db_list):
    if idx == 0:
        union_string += str(item)+"TablesList WHERE isTemporary = 'false'"
    else:
        union_string += " UNION ALL SELECT database, tableName FROM {}".format(str(item)+"TablesList WHERE isTemporary = 'false'")
spark.sql(union_string).createOrReplaceTempView("allTables")

# full list = schema, table, column
full_list = []
for i in spark.sql("SELECT * FROM allTables").collect():
    table_name = i[0]+"."+i[1]
    table_schema = spark.sql("SELECT * FROM {}".format(table_name))
    column_list = []
    for j in table_schema.schema:
        column_list.append(get_schema_field_name(j))
        column_list = flatten(column_list)
        for k in column_list:
                    full_list.append([i[0],i[1],k])
                    spark.createDataFrame(full_list, schema = ['database', 'tableName', 'columnName']).createOrReplaceTempView("allColumns")

# COMMAND ----------

df=sqlContext.sql("show tables in default")
tableList = [x["tableName"] for x in df.rdd.collect()]
for tbl in tableList:
    print(tbl)

# COMMAND ----------


