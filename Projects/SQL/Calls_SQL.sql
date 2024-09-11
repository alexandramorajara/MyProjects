BEGIN
 
-- We declare VARIABLES for the dates
 
DECLARE @StartDate DATETIME;
DECLARE @EndDate DATETIME;
 
SET @StartDate = DATEADD(day, -5, GETDATE());
SET @EndDate = DATEADD(day, -1, GETDATE());
 

------------------------------------------------------------------
-- CTE_Calls

WITH CTE_Calls AS (
 
SELECT
  1 AS [Calls_Count],
  Calls."s#comm" AS [Calls_Number],
  Installations_Spain.instalation_number AS [Installation_Number],
  Calls.Device AS [Device],

  -- Devices_types --
  CASE 
    WHEN Calls.Device IN ('XXXAST', 'XXXSW', 'XXXRP', 'AUVTYPE1') THEN 'TYPE1' 
    WHEN Calls.Device IN ('XXXCU', 'XXXCUW') THEN 'TYPE2'
    WHEN Calls.Device IN ('XXX-624-ES', 'XX 2000', 'XX 2000C', 'XX 2000CD', 'XX 2000D', 'VISTA25-LL', 'VISTA-25SP', 'XL20DIRECT', 'XL20-LL', 'XXP', 'XXM', 'XXMF', 'XXMG', 'XXMI', 'XXMK', 'XXML') THEN 'TYPE3'
    WHEN Calls.Device IN ('XXARIT', 'XXVISTA', 'XXRIXXO', 'XXMILL', 'XXSIEM', 'XXDXX', 'XXPARA', 'XXOTR', 'XXXL20', 'XXSIEM3', 'XXPARA3', 'XXOTHER' ) THEN 'TYPE4'
    WHEN Calls.Device = 'XXMOVIL' THEN 'TYPE5'
    WHEN Calls.Device IN ('TYPE6', 'TYPE6-D') THEN 'TYPE6'
    WHEN Calls.Device = 'XXROAD' THEN 'ONROAD'
    ELSE 'Others'
  END AS [Group_Device],


  -- Dates
  Dates.year AS [Year],
  Dates.Month_EN_ANO AS [Month],
  Dates.Dates AS [Dates],
  Convert(varchar(10),Calls.Dates) + ' ' +
  CASE 
  WHEN hour(Calls.Dates)<10 THEN '0'+convert(varchar(2),hour(Calls.Dates)) ELSE Convert(varchar(2),hour(Calls.Dates)) END + ':' +
  CASE WHEN minute(Calls.Dates)<10 THEN '0'+convert(varchar(2),minute(Calls.Dates)) ELSE Convert(varchar(2),minute(Calls.Dates)) END+ ':' +
  CASE WHEN second(Calls.Dates)<10 THEN '0'+convert(varchar(2),second(Calls.Dates)) ELSE Convert(varchar(2),second(Calls.Dates)) END AS [Dates_hour],

  -- Other information
  Calls.worker_ID AS [Worker_ID],
  Calls.business_ID AS [Business_ID],
  Calls.connid AS [Conn_ID],
  Calls.media AS [Media], 
  Source_Spain.source AS [Source],
  Calls.flag_critical AS [Flag_critical],
  CASE  
    WHEN Calls.direction='I'  THEN 'Incoming'  
    WHEN Calls.direction='O'  THEN 'Outgoing' 
    ELSE Calls.direction 
  END AS [Direction],
  Calls.text AS [Texts],
  Calls.key1 AS [Key1],
  Calls.key1txt AS [Texts_Key1],
  Calls.key2 AS [Key2],
  Calls.key2txt AS [Texts_Key2],
  Calls.key3 AS [Key3],
  Calls.key3txt AS [Texts_Key3],
  Calls.key4 AS [Key4],
  Calls.key4txt AS [Texts_Key4],
  Calls.key1 || '-' || Calls.key2 AS [Concatenate_Key1_Key2],
  Calls.key1 || '-' || Calls.key2 || '-'  || Calls.key3 AS [Concatenate_Key1_Key2_Key3],

  -- We create a column to remove duplicates.

  ROW_NUMBER() OVER (PARTITION BY Dates || Worker_ID || Installation_Number || Key1 || Key2 || Calls_Number ORDER BY Dates_hour DESC) AS [Calls_Duplicates],

  -- FIRST CALL RESOLUTION --
      CASE WHEN
      (
      Calls.direction = 'I' -- Incoming.
      AND
      Source_Spain.source IN  ('V', 'BAXX' ,'NXX','TC','AVS') 
      AND
      Calls.media  IN  ('PHONE','CHAT', 'RRSS') 
      AND
      Calls.key1 NOT IN ('BBNN', 'BBNNE', 'NNA')
      AND
      NOT (Calls.key1 = 'XXU' AND Calls.key2 <> 'XXU20')
      ) THEN 'FCR_CUSTOMER'
      ELSE 'NO_APLICA'
      END AS [FCR]


FROM

  (
  SELECT * 
  FROM server_name.database_name1 
  WHERE id_country=2) AS Calls -- Main table
  
  INNER JOIN (
  SELECT * 
  FROM server_name.database_name2 
  WHERE id_country=2) AS Source_Spain ON (Source_Spain.id_cod=Calls.cod_source) 
  
  INNER JOIN (
  SELECT * 
  FROM server_name.database_name3
  WHERE id_country=2) AS Installations_Spain ON (Installations_Spain.instalation_number=Calls.instalation_number) 
  
  INNER JOIN server_name.database_name4 AS Dates ON (Calls.id_Dates_crea=Dates.ID_Dates) 

WHERE
(

  Dates.Dates BETWEEN @StartDate AND @EndDate
  AND 
  (Calls.worker_ID = '3xxx' OR Calls.worker_ID LIKE '33%') 
  AND
  Calls.Device NOT IN ( 'AUVTYPE1-D','TYPE6-D','TYPE6','XXIP-DEMO','XXIRP-DEMO','XXM-DEMO','XXMF-DEMO','XXMG-DEMO','XXMI-DEMO','XXMK-DEMO','XXML-DEMO','XXP-DEMO','XXXR-DEMO','XXXRP-DMO','XXXRW-DMO','XXXAST-D','XXXSW-DMO','XXVISION-D','XXVSFUS-D','XXVSUN-D','TYPE6-D','VXXAMRF-D', 'XXXCU-D', 'XXXCUD') -- We remove demo devices
  AND
  Calls.key1 NOT IN  ('XAXX2', 'AEXX3', 'BBXX', 'SSSCE', 'XXVGR') 
  AND
  Installations_Spain.ins_no <> '-'
  AND
  Installations_Spain.ins_no NOT IN ('11111110', '11111111', '11111112', '11111115', '11111117')  
)

),


-- CTE_FCR 

CTE_FCR AS
  (
  SELECT 
  C1.Calls_Number,
  MIN(C2.Dates_hour) AS Next_date

  FROM 
  ( SELECT *
    FROM CTE_Calls
    WHERE
    CTE_Calls.FCR = 1
  ) AS C1 
  
  INNER JOIN 
  (SELECT *
    FROM CTE_Calls
    WHERE
    CTE_Calls.FCR = 1
  ) AS C2
  ON 
  (C2.Installation_Number = C1.Installation_Number
  AND C2.Dates_hour >= C1.Dates_hour
  AND C2.Dates >= C1.Dates
  AND C2.Key1 = C1.Key1
  AND C1.Calls_Duplicates = 1
  AND C2.Calls_Duplicates = 1
  AND C1.Calls_Number <> C2.Calls_Number
  )

  GROUP BY
  C1.Calls_Number

)


-----------------------------------------------------------------------------------------------------------------
-- MAIN QUERY

SELECT
  CTE_Calls.Calls_Count AS [Calls Count],
  CTE_Calls.Calls_Number AS [Calls Number],
  CTE_Calls.Installation_Number AS Installation Number],
  CTE_Calls.Device AS [Device],
  CTE_Calls.Group_Device AS [Group Device],

  -- Datos Dates
  CTE_Calls.Year AS [Year],
  CTE_Calls.Month AS [Month],
  CTE_Calls.Dates AS [Dates],
  CTE_Calls.Dates_hour AS [Dates hour],

  -- Others datos
  CTE_Calls.Worker_ID AS [Worker ID],
  CTE_Calls.Business_ID AS [Business ID],
  CTE_Calls.Conn_ID AS [ConnID],
  CTE_Calls.Media AS [Media], 
  CTE_Calls.Source AS [Source],
  CTE_Calls.Direction AS [Direction]
],
  CTE_Calls.Texts AS [Texts],
  CTE_Calls.Key1 AS [Key1],
  CTE_Calls.Texts_Key1 AS [Texts Key1],
  CTE_Calls.Key2 AS [Key2],
  CTE_Calls.Texts_Key2 AS [Texts Key2],
  CTE_Calls.Key3 AS [Key3],
  CTE_Calls.Texts_Key3 AS [Texts Key3],
  CTE_Calls.Key4 AS [Key4],
  CTE_Calls.Texts_Key4 AS [Texts Key4],
  CTE_Calls.Concatenate_Key1_Key2 AS [Concatenate Key1_Key2],
  CTE_Calls.Concatenate_Key1_Key2_Key3 AS [Concatenate Key1_Key2_Key3],
  CTE_Calls.Flag_critical AS [Flag critical],
  
  -- FCR 
  CTE_Calls.FCR AS [FCR],
  CASE
  WHEN (DATEDIFF(day, CTE_Calls.Dates_hour, CTE_FCR.Next_date)) > 0 AND (DATEDIFF(day, CTE_Calls.Dates_hour, CTE_FCR.Next_date)) <= 30 
  THEN 1
  ELSE 0
  END AS [FLAG_FCR]


FROM
  CTE_Calls

  LEFT JOIN CTE_FCR ON (CTE_Calls.Calls_Number  = CTE_FCR.Calls_Number)

WHERE

  CTE_Calls.Calls_Duplicates = 1

END


