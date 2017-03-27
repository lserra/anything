%spark
// start the Spark context

// create a Spark context object
z.sc

import org.apache.spark.sql.hive.HiveContext

val sqlContext = new HiveContext(z.sc)

// enable the following properties to make it work
sqlContext.setConf("hive.exec.dynamic.partition", "true") 
sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict") 

// create a SQL that will return a DataFrame with the data values to insert into the Hive table
var strSQL = """
INSERT OVERWRITE TABLE DW_BUYBOX.BBOX_LOG_DIARY PARTITION(EXTRACTION_DATE_P)
SELECT 
  API.PARTITION_DATE AS EXTRACTION_DATE
  , CASE 
    WHEN UPPER(API.LOAD_SUBJECT) = 'SORTING-DATA-SUBA' THEN 'SUBA'
    WHEN UPPER(API.LOAD_SUBJECT) = 'SORTING-DATA-SHOP' THEN 'SHOP'
    WHEN UPPER(API.LOAD_SUBJECT) = 'SORTING-DATA' THEN 'ACOM'
    END AS BRAND_NAME
  , COALESCE(MPH.DEPA_NOME, '1-SI') AS DEPARTMENT
  , API.SELLERID AS BOXSEL_SELLER_ID
  , CASE  WHEN CAST(API.RATINGVALUE AS DOUBLE) >= 0   AND CAST(API.RATINGVALUE AS DOUBLE) <= 1.0 THEN 'ENTRE 0 E 1.0'
            WHEN CAST(API.RATINGVALUE AS DOUBLE) >= 1.1 AND CAST(API.RATINGVALUE AS DOUBLE) <= 2.0 THEN 'ENTRE 1.1 E 2.0'
            WHEN CAST(API.RATINGVALUE AS DOUBLE) >= 2.1 AND CAST(API.RATINGVALUE AS DOUBLE) <= 3.0 THEN 'ENTRE 2.1 E 3.0'
            WHEN CAST(API.RATINGVALUE AS DOUBLE) >= 3.1 AND CAST(API.RATINGVALUE AS DOUBLE) <= 4.0 THEN 'ENTRE 3.1 E 4.0'
            WHEN CAST(API.RATINGVALUE AS DOUBLE) >= 4.1 AND CAST(API.RATINGVALUE AS DOUBLE) < 5.0 THEN 'ENTRE 4.1 E 4.9'
            WHEN CAST(API.RATINGVALUE AS DOUBLE) = 5.0 THEN 'NOTA 5.0'
            ELSE 'SI'
      END AS BOXSEL_SCORE_RATING  
  , COUNT(DISTINCT API.PRODUCTID) AS BOXAGR_PRODUCTS_QTY
  , MAX(API.RATINGVALUE) AS BOXSEL_RATING
  , COUNT(*) AS BOXSEL_TOTAL_CASES
  , SUM(CASE WHEN CAST(API.POSITION AS INT) = 0 
             THEN 1 
             ELSE 0 
        END) AS BOXSEL_TOTAL_WINNERS
  , SUM(CASE WHEN API.SELLERID = '00776574000660' AND CAST(API.POSITION AS INT) = 0 
             THEN 1 
             ELSE 0 
        END) AS BOXSEL_1P_WINNER
  , SUM(CASE WHEN NOT (API.SELLERID = '00776574000660') AND CAST(API.POSITION AS INT) = 0 
             THEN 1
             ELSE 0
        END) AS BOXSEL_3P_WINNER
  , SUM(CASE WHEN CAST(API.NORMALIZEDLANDEDMINPRICE AS FLOAT) = 1.0
             THEN 1
             ELSE 0 END
       ) AS BOXSEL_LOW_PRICE
  , SUM(CASE WHEN CAST(API.POSITION AS INT) = 0 AND CAST(API.NORMALIZEDLANDEDMINPRICE AS FLOAT) = 1.0
             THEN 1
             ELSE 0
        END) AS BOXSEL_FIRST_AND_LOW_PRICE 
  , SUM(CASE WHEN API.SELLERID = '00776574000660' AND CAST(API.POSITION AS INT) = 0 THEN 1 ELSE 0 END) AS BOXAGR_1P_WINNER
  , SUM(CASE WHEN NOT (API.SELLERID = '00776574000660') AND CAST(API.POSITION AS INT) = 0 THEN 1 ELSE 0 END) AS BOXAGR_3P_WINNER
  , SUM(CASE WHEN API.SELLERID = '00776574000660' AND CAST(API.POSITION AS INT) = 0 THEN 1 ELSE 0 END) AS BOXAGR_1P_MIX_WINNER
  , SUM(CASE WHEN API.SELLERID = '00776574000660' THEN 1 ELSE 0 END) - SUM(CASE WHEN API.SELLERID = '00776574000660' AND 
      CAST(API.POSITION AS INT) = 0 
    THEN 1 ELSE 0 END) AS BOXAGR_3P_MIX_WINNER
  , SUM(CASE WHEN API.SELLERID = '00776574000660' THEN 1 ELSE 0 END) AS BOXAGR_BBOX_1P_3P
  , SUM(CASE WHEN LOAD_TIMESTAMP < '2999-01-01' AND CAST(API.RATINGVALUE AS DOUBLE) <= 3.5 THEN 1 ELSE 0 
       END) AS RATING_UNDER_MINIMUM
  , API.PARTITION_DATE
FROM LOGS_FRONT.LOG_BUYBOX_API API

LEFT JOIN DATALAKE_UMBRELLA.UMBRELLA_DEPARTAMENTO MPH
ON API.DEPARTMENTID = MPH.DEPA_ID_DEPTO
AND MPH.DEPA_ID_CIA = '1'

WHERE API.PARTITION_DATE >= DATE_SUB(TO_DATE(CURRENT_DATE), 1)
GROUP BY 
     API.LOAD_SUBJECT
   , API.PARTITION_DATE
   , MPH.DEPA_NOME
   , API.SELLERID
   , CASE   WHEN CAST(API.RATINGVALUE AS DOUBLE) >= 0   AND CAST(API.RATINGVALUE AS DOUBLE) <= 1.0 THEN 'ENTRE 0 E 1.0'
            WHEN CAST(API.RATINGVALUE AS DOUBLE) >= 1.1 AND CAST(API.RATINGVALUE AS DOUBLE) <= 2.0 THEN 'ENTRE 1.1 E 2.0'
            WHEN CAST(API.RATINGVALUE AS DOUBLE) >= 2.1 AND CAST(API.RATINGVALUE AS DOUBLE) <= 3.0 THEN 'ENTRE 2.1 E 3.0'
            WHEN CAST(API.RATINGVALUE AS DOUBLE) >= 3.1 AND CAST(API.RATINGVALUE AS DOUBLE) <= 4.0 THEN 'ENTRE 3.1 E 4.0'
            WHEN CAST(API.RATINGVALUE AS DOUBLE) >= 4.1 AND CAST(API.RATINGVALUE AS DOUBLE) < 5.0 THEN 'ENTRE 4.1 E 4.9'
            WHEN CAST(API.RATINGVALUE AS DOUBLE) = 5.0 THEN 'NOTA 5.0'
            ELSE 'SI'
      END"""

println(strSQL)

// save the DataFrame into the Hive table
val result_sql = sqlContext.sql(strSQL)

// send a message to control and monitor the activities from this script
println("New records has been inserted!")

// declare and set the variables
var num_records: Long = 0

// create the SQL that will return the number of records from table dw_buybox.bbox_log_diary
var strSQL = "select count(distinct api.extraction_date) as num_records "
strSQL += "from dw_buybox.bbox_log_diary api "

println(strSQL)

// execute the SQL above
var result_sql = sqlContext.sql(strSQL)

println(result_sql)

// get the total records and assign a new variable
num_records = result_sql.collect()(0)(0).asInstanceOf[Long]         

println(num_records)

// search the min date
var strSQL = "select min(api.extraction_date) as dt_extraction "
strSQL += "from dw_buybox.bbox_log_diary api "

println(strSQL)

// execute the SQL above to find out the min date
var result_sql = sqlContext.sql(strSQL)

println(result_sql)

// delete the oldest records (based in the min date)
var strSQL = "delete "
strSQL += "from dw_buybox.bbox_log_diary api "
strSQL += "where api.extraction_date_p in "
strSQL += "(select min(api.extraction_date) as dt_extraction "
strSQL += "from dw_buybox.bbox_log_diary api)"

println(strSQL)

if( num_records > 30 ){

	// execute the SQL above to delete the records
	var result_sql = sqlContext.sql(strSQL)

	// send a message to control and monitor the activities from this script
	println("Old records has been deleted!")

		} 

// send a message to control and monitor the activities from this script
println("Job finished!")