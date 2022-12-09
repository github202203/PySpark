/**********************************************************************************************************************************************************
   Version  |  Date        | Author              | Description                                                                  
--------------------------------------------------------------------------------------------------------------------------------------------- 
   Entity:PolicyLimit
   
   0.1      |  19/11/2022  | Ram Baskar          | Script to validate Transformation Rule between Bronze Staging - Silver Staging
   0.2		|   6/12/2022  | Sumit Thakur        | Modified script to include TopLimitInd column and its transformation logic 
   
**********************************************************************************************************************************************************/


DECLARE @SystemLoadID    BIGINT = ''
       ,@Layer          VARCHAR(20)   = 'Silver'
       ,@TestType       NVARCHAR(MAX) = 'Transformation Rule Testng'
       ,@SchemaName        NVARCHAR(MAX) = 'dbo'
       ,@TableName        NVARCHAR(MAX) = 'PolicyLimit'
;

SET @SystemLoadId      = (SELECT SystemLoadId  FROM QAData.dbo.QAControlData WHERE SourceObjectName = @SchemaName + '.' + @TableName and SystemLoadId like '102%')
;


WITH  CTE_Source as (
SELECT [PolicyLimitId]                                                                  as SRC_PolicyLimitID
       ,[PolicyId]                                                                       as SRC_PolicyID
       ,[Limit]                                                                          as SRC_Limit
       ,CAST( CASE WHEN TopLimitInd = 'Y' THEN 1
        WHEN TopLimitInd = 'N' THEN 0
        WHEN TopLimitInd IS NULL THEN NULL
   END AS BIT )                                                                          as SRC_TopLimitInd
       ,CAST([DelDate] as datetime2)                                                     as SRC_DelDate
       ,(SELECT mdmCode FROM ProfiseeBronze.dbo.Ref_SourceSystem WHERE name = 'Eclipse') as SRC_SourceSystemCode
       ,[SYS_CHANGE_OPERATION]                                                           as SRC_SYS_CHANGE_OPERATION
       ,CAST(SPQ.filepath(1) AS BIGINT)                                                  as SRC_SystemLoadID
   FROM OPENROWSET(BULK 'https://dldpdev01.dfs.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/dbo_PolicyLimit/SystemLoadID=*/*.parquet',FORMAT = 'PARQUET') AS SPQ
)

,CTE_Target as (
SELECT [PolicyLimitID]                                                                  as TGT_PolicyLimitID
       ,[PolicyID]                                                                       as TGT_PolicyID
       ,[Limit]                                                                          as TGT_Limit
       ,TopLimitInd                                                                         as TGT_TopLimitInd
       ,CAST([DelDate] as datetime2)                                                     as TGT_DelDate
       ,(SELECT mdmCode FROM ProfiseeBronze.dbo.Ref_SourceSystem WHERE name = 'Eclipse') as TGT_SourceSystemCode
       ,[SYS_CHANGE_OPERATION]                                                           as TGT_SYS_CHANGE_OPERATION
       ,CAST(TPQ.filepath(1) AS BIGINT)                                                  as TGT_SystemLoadID
   FROM OPENROWSET(BULK 'https://dldpdev01.dfs.core.windows.net/silver/underwriting/Internal/Eclipse/Staging/dbo_PolicyLimit/SystemLoadID=*/*.parquet',FORMAT = 'PARQUET') AS TPQ
)

,ExpectedResult AS (

SELECT  @@ServerName as Server,  @Layer as Layer, @SchemaName + '.' + @TableName as Entity, @TestType as TestType,GetDate() as TestedOn
       ,Case when SRC_HB = TGT_HB then 'Passed' else 'Failed' End as TestResult
       ,SRC.*,TGT.*
  FROM (

      SELECT
               SRC_PolicyLimitID
              ,SRC_PolicyID
              ,SRC_Limit
              ,SRC_TopLimitInd
              ,SRC_DelDate
              ,SRC_SourceSystemCode
              ,SRC_SYS_CHANGE_OPERATION
              ,SRC_SystemLoadID
              --,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT(SRC_PolicyLimitID,'%',SRC_PolicyID,'%',SRC_Limit,SRC_TopLimitInd,'%','%',SRC_DelDate,'%',SRC_SourceSystemCode COLLATE SQL_Latin1_General_CP1_CI_AS))) as SRC_HB
              --With NO Collation
              ,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT(SRC_PolicyLimitID,'%',SRC_PolicyID,'%',SRC_Limit,'%',SRC_TopLimitInd,'%',SRC_DelDate,'%',SRC_SourceSystemCode))) as SRC_HB
          FROM CTE_Source
          WHERE SRC_SystemLoadID = 1022022120601
        ) SRC

      Left Join
        (

      SELECT
               TGT_PolicyLimitID
              ,TGT_PolicyID
              ,TGT_Limit
              ,TGT_TopLimitInd
              ,TGT_DelDate
              ,TGT_SourceSystemCode
              ,TGT_SYS_CHANGE_OPERATION
              ,TGT_SystemLoadID
              --,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT(TGT_PolicyLimitID,'%',TGT_PolicyID,'%',TGT_Limit,TGT_TopLimitInd,'%',TGT_DelDate,'%',TGT_SourceSystemCode COLLATE SQL_Latin1_General_CP1_CI_AS))) as TGT_HB
              --With NO Collation
              ,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT(TGT_PolicyLimitID,'%',TGT_PolicyID,'%',TGT_Limit,'%',TGT_TopLimitInd,'%',TGT_DelDate,'%',TGT_SourceSystemCode))) as TGT_HB
          FROM CTE_Target
          --WHERE TGT_SystemLoadID = @SystemLoadID
        ) TGT
          on SRC_PolicyLimitID = TGT_PolicyLimitID

     )

SELECT * FROM ExpectedResult

