-- DIARY
SELECT PARTITION_DATE AS EXTRACTION_DATE  
       , LOAD_SUBJECT AS MARCA
       , COUNT(DISTINCT SELLERID) AS NUM_SELLERS
       , COUNT(DISTINCT PRODUCTID) AS PRODUTOS_GERAL
       , COUNT(DISTINCT CASE WHEN SELLERID = '00776574000660' THEN PRODUCTID ELSE NULL END) AS PRODUTOS_1P_3P
       , SUM(CASE WHEN CAST(POSITION AS INT) = 0 THEN 1 ELSE 0 END) AS TOTAL_CASOS      
       , SUM(CASE WHEN SELLERID = '00776574000660' THEN 1 ELSE 0 END) AS BBOX_1P_3P
       , SUM(CASE WHEN CAST(POSITION AS INT) = 0 THEN 1 ELSE 0 END)
       - SUM(CASE WHEN SELLERID = '00776574000660' THEN 1 ELSE 0 END) AS BBOX_3P
       , SUM(CASE WHEN SELLERID = '00776574000660' 
             AND CAST(POSITION AS INT) = 0 THEN 1 ELSE 0 END) AS 1P_VENCEDOR
       , SUM(CASE WHEN NOT (SELLERID = '00776574000660') 
             AND CAST(POSITION AS INT) = 0 THEN 1 ELSE 0 END) AS 3P_VENCEDOR
       , SUM(CASE WHEN SELLERID = '00776574000660' 
             AND CAST(POSITION AS INT) = 0 THEN 1 ELSE 0 END) AS 1P_VENCEDOR_MISTO
       , SUM(CASE WHEN SELLERID = '00776574000660' THEN 1 ELSE 0 END)
       - SUM(CASE WHEN SELLERID = '00776574000660' 
             AND CAST(POSITION AS INT) = 0 THEN 1 ELSE 0 END) AS 3P_VENCEDOR_MISTO
       , SUM(CASE WHEN SELLERID = '00776574000660' 
             AND CAST(NORMALIZEDLANDEDPRICE AS INT) = 1 
             AND CAST(POSITION AS INT) = 0 THEN 1 ELSE 0 END) AS 1P_VENCEDOR_BARATO             
       , SUM(CASE WHEN NOT (SELLERID = '00776574000660') 
             AND CAST(NORMALIZEDLANDEDPRICE AS INT) = 1 
             AND CAST(POSITION AS INT) = 0 THEN 1 ELSE 0 END) AS 3P_VENCEDOR_BARATO             
       , SUM(CASE WHEN CAST(NORMALIZEDLANDEDPRICE AS INT) = 1 
             AND CAST(POSITION AS INT) = 0 THEN 1 ELSE 0 END) AS VENCEDOR_BARATO           
       , COUNT(*) AS REGISTROS
FROM   LOGS_FRONT.LOG_BUYBOX_API
WHERE  PARTITION_DATE = '2016-11-05'
GROUP  BY PARTITION_DATE, LOAD_SUBJECT


-- SELLER
SELECT   SELLERID
       , LOAD_SUBJECT
       , SUM(CASE WHEN CAST(NORMALIZEDLANDEDPRICE AS FLOAT) = 1.0 THEN 1 ELSE 0 END) AS MENOR_PRECO
       , SUM(CASE WHEN CAST(POSITION AS INT) = 0 AND 
             CAST(NORMALIZEDLANDEDPRICE AS FLOAT) = 1.0 THEN 1 ELSE 0 END) AS PRIMEIRO_E_MENOR_PRECO
       , COUNT(DISTINCT PRODUCTID) AS NUM_PRODUCTS
       , SUM(CASE WHEN CAST(POSITION AS INT) = 0 THEN 1 ELSE 0 END) AS PRIMEIRO
       , SUM(CASE WHEN CAST(POSITION AS INT) = 1 THEN 1 ELSE 0 END) AS SEGUNDO
       , SUM(CASE WHEN CAST(POSITION AS INT) = 2 THEN 1 ELSE 0 END) AS TERCEIRO
       , SUM(CASE WHEN CAST(POSITION AS INT) = 3 THEN 1 ELSE 0 END) AS QUARTO
       , SUM(CASE WHEN CAST(POSITION AS INT) = 4 THEN 1 ELSE 0 END) AS QUINTO
       , SUM(CASE WHEN CAST(POSITION AS INT) = 5 THEN 1 ELSE 0 END) AS SEXTO
       , SUM(CASE WHEN CAST(POSITION AS INT) = 6 THEN 1 ELSE 0 END) AS SETIMO
       , SUM(CASE WHEN CAST(POSITION AS INT) = 7 THEN 1 ELSE 0 END) AS OITAVO
       , SUM(CASE WHEN CAST(POSITION AS INT) = 8 THEN 1 ELSE 0 END) AS NONO
       , SUM(CASE WHEN CAST(POSITION AS INT) = 9 THEN 1 ELSE 0 END) AS DECIMO
       , SUM(CASE WHEN CAST(POSITION AS INT) > 9 THEN 1 ELSE 0 END) AS DEMAIS
       , COUNT(*) AS TOTAL
FROM   LOG_BUYBOX_API 
WHERE  PARTITION_DATE = '2016-11-11'
GROUP  BY SELLERID
       , LOAD_SUBJECT