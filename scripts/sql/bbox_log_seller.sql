-- HIVE
-- INSERT INTO BBOX_LOG_SELLER PARTITION (BOXSEL_EXTRACTION_DATE_P='2016-09-07')
-- SELECT   PARTITION_DATE AS BOXSEL_EXTRACTION_DATE
--        , SELLERID AS BOXSEL_SELLER_ID
--        , LOAD_SUBJECT AS BOXSEL_BRAND_NAME
--        , SUM(CASE WHEN CAST(NORMALIZEDLANDEDPRICE AS FLOAT) = 1.0 THEN 1 ELSE 0 END) AS BOXSEL_LOW_PRICE
--        , SUM(CASE WHEN CAST(POSITION AS INT) = 0 AND CAST(NORMALIZEDLANDEDPRICE AS FLOAT) = 1.0 THEN 1 ELSE 0 END) AS BOXSEL_FIRST_AND_LOW_PRICE
--        , COUNT(DISTINCT PRODUCTID) AS BOXSEL_QTY_PRODUCTS
--        , SUM(CASE WHEN CAST(POSITION AS INT) = 0 THEN 1 ELSE 0 END) AS BOXSEL_FIRST
--        , SUM(CASE WHEN CAST(POSITION AS INT) = 1 THEN 1 ELSE 0 END) AS BOXSEL_SECOND
--        , SUM(CASE WHEN CAST(POSITION AS INT) = 2 THEN 1 ELSE 0 END) AS BOXSEL_THIRD
--        , SUM(CASE WHEN CAST(POSITION AS INT) = 3 THEN 1 ELSE 0 END) AS BOXSEL_FOURTH
--        , SUM(CASE WHEN CAST(POSITION AS INT) = 4 THEN 1 ELSE 0 END) AS BOXSEL_FIFTH
--        , SUM(CASE WHEN CAST(POSITION AS INT) = 5 THEN 1 ELSE 0 END) AS BOXSEL_SIXTH
--        , SUM(CASE WHEN CAST(POSITION AS INT) = 6 THEN 1 ELSE 0 END) AS BOXSEL_SEVENTH
--        , SUM(CASE WHEN CAST(POSITION AS INT) = 7 THEN 1 ELSE 0 END) AS BOXSEL_EIGHT
--        , SUM(CASE WHEN CAST(POSITION AS INT) = 8 THEN 1 ELSE 0 END) AS BOXSEL_NINETH
--        , SUM(CASE WHEN CAST(POSITION AS INT) = 9 THEN 1 ELSE 0 END) AS BOXSEL_TENTH
--        , SUM(CASE WHEN CAST(POSITION AS INT) > 9 THEN 1 ELSE 0 END) AS BOXSEL_OTHERS
--        , COUNT(*) AS BOXSEL_TOTAL
-- FROM   LOGS_FRONT.LOG_BUYBOX_API 
-- WHERE  PARTITION_DATE = '2016-09-07'
-- GROUP  BY SELLERID
--        , LOAD_SUBJECT
--        , PARTITION_DATE;

SET HIVE.EXEC.DYNAMIC.PARTITION.MODE=NOSTRICT;

INSERT OVERWRITE TABLE DW_BUYBOX.BBOX_LOG_SELLER PARTITION(BOXSEL_EXTRACTION_DATE_P)
SELECT API.PARTITION_DATE AS BOXSEL_EXTRACTION_DATE
       , API.SELLERID AS BOXSEL_SELLER_ID
       , API.LOAD_SUBJECT AS BOXSEL_BRAND_NAME
       , SUM(CASE WHEN CAST(API.NORMALIZEDLANDEDPRICE AS FLOAT) = 1.0 THEN 1 ELSE 0 END) AS BOXSEL_LOW_PRICE
       , SUM(CASE WHEN CAST(API.POSITION AS INT) = 0 AND CAST(API.NORMALIZEDLANDEDPRICE AS FLOAT) = 1.0 THEN 1 ELSE 0 END) AS BOXSEL_FIRST_AND_LOW_PRICE
       , COUNT(DISTINCT API.PRODUCTID) AS BOXSEL_QTY_PRODUCTS
       , SUM(CASE WHEN CAST(API.POSITION AS INT) = 0 THEN 1 ELSE 0 END) AS BOXSEL_FIRST
       , SUM(CASE WHEN CAST(API.POSITION AS INT) = 1 THEN 1 ELSE 0 END) AS BOXSEL_SECOND
       , SUM(CASE WHEN CAST(API.POSITION AS INT) = 2 THEN 1 ELSE 0 END) AS BOXSEL_THIRD
       , SUM(CASE WHEN CAST(API.POSITION AS INT) = 3 THEN 1 ELSE 0 END) AS BOXSEL_FOURTH
       , SUM(CASE WHEN CAST(API.POSITION AS INT) = 4 THEN 1 ELSE 0 END) AS BOXSEL_FIFTH
       , SUM(CASE WHEN CAST(API.POSITION AS INT) = 5 THEN 1 ELSE 0 END) AS BOXSEL_SIXTH
       , SUM(CASE WHEN CAST(API.POSITION AS INT) = 6 THEN 1 ELSE 0 END) AS BOXSEL_SEVENTH
       , SUM(CASE WHEN CAST(API.POSITION AS INT) = 7 THEN 1 ELSE 0 END) AS BOXSEL_EIGHT
       , SUM(CASE WHEN CAST(API.POSITION AS INT) = 8 THEN 1 ELSE 0 END) AS BOXSEL_NINETH
       , SUM(CASE WHEN CAST(API.POSITION AS INT) = 9 THEN 1 ELSE 0 END) AS BOXSEL_TENTH
       , SUM(CASE WHEN CAST(API.POSITION AS INT) > 9 THEN 1 ELSE 0 END) AS BOXSEL_OTHERS
       , COUNT(*) AS BOXSEL_TOTAL
     , API.PARTITION_DATE
  FROM LOGS_FRONT.LOG_BUYBOX_API API
  JOIN (SELECT CASE WHEN DATE_ADD(MAX(BOXSEL_EXTRACTION_DATE_P),1) >= SUBSTRING(CURRENT_DATE,1,10) 
                    THEN DATE_ADD(MAX(BOXSEL_EXTRACTION_DATE_P),0) 
                    ELSE DATE_ADD(MAX(BOXSEL_EXTRACTION_DATE_P),1) 
               END  AS BOXSEL_EXTRACTION_DATE_P
          FROM DW_BUYBOX.BBOX_LOG_SELLER
       ) MAX_DATE
WHERE  API.PARTITION_DATE = MAX_DATE.BOXSEL_EXTRACTION_DATE_P
GROUP  BY 
         API.SELLERID
       , API.LOAD_SUBJECT
       , API.PARTITION_DATE;