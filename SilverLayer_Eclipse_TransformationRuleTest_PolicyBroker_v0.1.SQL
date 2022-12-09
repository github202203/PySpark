/**********************************************************************************************************************************************************
   Version  |  Date        | Author              | Description                                                                  
--------------------------------------------------------------------------------------------------------------------------------------------- 
   Entity:PolicyBroker
   
   0.1      |  06/12/2022  | Sumit Thakur          | Script to validate Transformation Rule between Bronze Staging - Silver Staging
   
**********************************************************************************************************************************************************/


DECLARE @SystemLoadID	BIGINT = ''
       ,@Layer          VARCHAR(20)   = 'Silver'
       ,@TestType       NVARCHAR(MAX) = 'Transformation Rule Testng'
       ,@SchemaName	    NVARCHAR(MAX) = 'dbo'
       ,@TableName	    NVARCHAR(MAX) = 'PolicyBroker'
;

SET @SystemLoadId      = (SELECT SystemLoadId  FROM QAData.dbo.QAControlData WHERE SourceObjectName = @SchemaName + '.' + @TableName and SystemLoadId like '102%')

;

WITH 

 CTE_Source as (
 SELECT [PolicyBrokerID]                                                                 as SRC_PolicyBrokerID
	   ,(SELECT mdmCode FROM ProfiseeBronze.dbo.Ref_SourceSystem WHERE name = 'Eclipse') as SRC_SourceSystemCode
	   ,LloydsBrokerID 												 					 as SRC_LlyodsBrokerID
       ,[PolicyID]                                                                       as SRC_PolicyID
       ,CAST([DelDate] as datetime2)                                                     as SRC_DelDate
	   ,[HierarchyOrder]                                                                 as SRC_HierarchyOrder
	   ,[SYS_CHANGE_OPERATION]                                                           as SRC_SYS_CHANGE_OPERATION
	   ,CAST(SPQ.filepath(1) AS BIGINT)                                                  as SRC_SystemLoadID
   FROM OPENROWSET(BULK 'https://dldpdev01.dfs.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/dbo_PolicyBroker/SystemLoadID=*/*.parquet',FORMAT = 'PARQUET') AS SPQ
 )

,CTE_Target as (
 SELECT [PolicyBrokerID]                                                                 as TGT_PolicyBrokerID
	   ,(SELECT mdmCode FROM ProfiseeBronze.dbo.Ref_SourceSystem WHERE name = 'Eclipse') as TGT_SourceSystemCode
	   ,LloydsBrokerID 											 					     as TGT_LlyodsBrokerID
       ,[PolicyID]                                                                       as TGT_PolicyID
       ,CAST([DelDate] as datetime2)                                                     as TGT_DelDate
	   ,[HierarchyOrder]                                                                 as TGT_HierarchyOrder
	   ,[SYS_CHANGE_OPERATION]                                                           as TGT_SYS_CHANGE_OPERATION
	   ,CAST(TPQ.filepath(1) AS BIGINT)                                                  as TGT_SystemLoadID
   FROM OPENROWSET(BULK 'https://dldpdev01.dfs.core.windows.net/silver/underwriting/Internal/Eclipse/Staging/dbo_PolicyBroker/SystemLoadID=*/*.parquet',FORMAT = 'PARQUET') AS TPQ
 )


,ExpectedResult AS (

SELECT  @@ServerName as Server,  @Layer as Layer, @SchemaName + '.' + @TableName as Entity, @TestType as TestType,GetDate() as TestedOn
       ,Case when SRC_HB = TGT_HB then 'Passed' else 'Failed' End as TestResult
	   ,SRC.*,TGT.*
  FROM (

        SELECT 
               SRC_PolicyBrokerID
			  ,SRC_SourceSystemCode
			  ,SRC_LlyodsBrokerID
              ,SRC_PolicyID
              ,SRC_DelDate
			  ,SRC_HierarchyOrder
              ,SRC_SYS_CHANGE_OPERATION
              ,SRC_SystemLoadID
			  --,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT(SRC_PolicyBrokerID,'%',SRC_SourceSystemCode,'%',SRC_LlyodsBrokerID,'%',SRC_PolicyID,'%',SRC_DelDate,'%',SRC_HierarchyOrder COLLATE SQL_Latin1_General_CP1_CI_AS))) as SRC_HB
              --With NO Collation
              ,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT(SRC_PolicyBrokerID,'%',SRC_SourceSystemCode,'%',SRC_LlyodsBrokerID,'%',SRC_PolicyID,'%',SRC_DelDate,'%',SRC_HierarchyOrder))) as SRC_HB
          FROM CTE_Source
		  WHERE SRC_SystemLoadID = 1022022120601
        ) SRC

		Left Join 
		(

        SELECT 
               TGT_PolicyBrokerID
			  ,TGT_SourceSystemCode
			  ,TGT_LlyodsBrokerID
              ,TGT_PolicyID
              ,TGT_DelDate
			  ,TGT_HierarchyOrder
              ,TGT_SYS_CHANGE_OPERATION
              ,TGT_SystemLoadID
			  --,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT(TGT_PolicyBrokerID,'%',TGT_SourceSystemCode,'%',TGT_LlyodsBrokerID,'%',TGT_PolicyID,'%',TGT_DelDate,'%',TGT_HierarchyOrder COLLATE SQL_Latin1_General_CP1_CI_AS))) as TGT_HB
              --With NO Collation
              ,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT(TGT_PolicyBrokerID,'%',TGT_SourceSystemCode,'%',TGT_LlyodsBrokerID,'%',TGT_PolicyID,'%',TGT_DelDate,'%',TGT_HierarchyOrder))) as TGT_HB
          FROM CTE_Target
		  --WHERE TGT_SystemLoadID = @SystemLoadID
        ) TGT
		  on SRC_PolicyBrokerID = TGT_PolicyBrokerID

	   )

SELECT * FROM ExpectedResult

