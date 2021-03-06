SET HIVE.EXEC.DYNAMIC.PARTITION.MODE=NOSTRICT;

INSERT OVERWRITE TABLE DW_BUYBOX.BBOX_LOG_AGR PARTITION(BOXAGR_EXTRACTION_DATE_P)
SELECT PARTITION_DATE AS BOXAGR_EXTRACTION_DATE
     , LOAD_SUBJECT   AS BOXAGR_BRAND_NAME
     , COUNT(DISTINCT SELLERID) AS BOXAGR_SELLERS_QTY
     , COUNT(DISTINCT PRODUCTID) AS BOXAGR_PRODUCTS_QTY
     , COUNT(DISTINCT CASE WHEN SELLERID = '00776574000660' THEN PRODUCTID ELSE NULL END) AS BOXAGR_XP_PRODUCTS_1P_3P
     , SUM(CASE WHEN CAST(POSITION AS INT) = 0   THEN 1 ELSE 0 END) AS BOXAGR_TOTAL_CASES      
     , SUM(CASE WHEN SELLERID = '00776574000660' THEN 1 ELSE 0 END) AS BOXAGR_BBOX_1P_3P
     , SUM(CASE WHEN CAST(POSITION AS INT) = 0   THEN 1 ELSE 0 END) - SUM(CASE WHEN SELLERID = '00776574000660' THEN 1 ELSE 0 END) AS BOXAGR_BBOX_3P
     , SUM(CASE WHEN SELLERID = '00776574000660'       AND CAST(POSITION AS INT) = 0 THEN 1 ELSE 0 END) AS BOXAGR_1P_WINNER
     , SUM(CASE WHEN NOT (SELLERID = '00776574000660') AND CAST(POSITION AS INT) = 0 THEN 1 ELSE 0 END) AS BOXAGR_3P_WINNER
     , SUM(CASE WHEN SELLERID = '00776574000660'       AND CAST(POSITION AS INT) = 0 THEN 1 ELSE 0 END) AS BOXAGR_1P_MIX_WINNER
     , SUM(CASE WHEN SELLERID = '00776574000660' THEN 1 ELSE 0 END) - SUM(CASE WHEN SELLERID = '00776574000660' AND CAST(POSITION AS INT) = 0 THEN 1 ELSE 0 END) AS BOXAGR_3P_MIX_WINNER
     , SUM(CASE WHEN SELLERID = '00776574000660' AND CAST(NORMALIZEDLANDEDPRICE AS INT) = 1 AND CAST(POSITION AS INT) = 0 THEN 1 ELSE 0 END) AS BOXAGR_1P_CHEAP_WINNER             
     , SUM(CASE WHEN NOT (SELLERID = '00776574000660') AND CAST(NORMALIZEDLANDEDPRICE AS INT) = 1 AND CAST(POSITION AS INT) = 0 THEN 1 ELSE 0 END) AS BOXAGR_3P_CHEAP_WINNER             
     , SUM(CASE WHEN CAST(NORMALIZEDLANDEDPRICE AS INT) = 1 AND CAST(POSITION AS INT) = 0 THEN 1 ELSE 0 END) AS BOXAGR_CHEAP_WINNER           
     , COUNT(*) AS BOXAGR_RECORDS_QTY
     , PARTITION_DATE
  FROM LOGS_FRONT.LOG_BUYBOX_API API
  JOIN (SELECT CASE WHEN DATE_ADD(MAX(BOXAGR_EXTRACTION_DATE_P),1) >= SUBSTRING(CURRENT_DATE,1,10) 
                    THEN DATE_ADD(MAX(BOXAGR_EXTRACTION_DATE_P),0) 
                    ELSE DATE_ADD(MAX(BOXAGR_EXTRACTION_DATE_P),1) 
               END  AS BOXAGR_EXTRACTION_DATE_P
          FROM DW_BUYBOX.BBOX_LOG_AGR
       ) MAX_DATE
WHERE  API.PARTITION_DATE = MAX_DATE.BOXAGR_EXTRACTION_DATE_P
 GROUP  BY API.PARTITION_DATE
     , API.LOAD_SUBJECT;