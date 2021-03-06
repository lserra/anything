-- DW_BUYBOX.BBOX_LOG_AGR
SELECT
	BOXAGR_1P_WINNER, 
	BOXAGR_EXTRACTION_DATE, 
	BOXAGR_SELLERS_QTY, 
	BOXAGR_PRODUCTS_QTY, 
	BOXAGR_XP_PRODUCTS_1P_3P, 
	BOXAGR_TOTAL_CASES, 
	BOXAGR_BBOX_1P_3P, 
	BOXAGR_BBOX_3P, 
	BOXAGR_BRAND_NAME, 
	BOXAGR_3P_WINNER, 
	BOXAGR_1P_MIX_WINNER, 
	BOXAGR_3P_MIX_WINNER, 
	BOXAGR_1P_CHEAP_WINNER, 
	BOXAGR_3P_CHEAP_WINNER, 
	BOXAGR_CHEAP_WINNER, 
	BOXAGR_RECORDS_QTY, 
	BOXAGR_EXTRACTION_DATE_P
FROM DW_BUYBOX.BBOX_LOG_AGR
-- HIVE
WHERE BOXAGR_EXTRACTION_DATE_P BETWEEN DATE_SUB(TO_DATE(CURRENT_DATE), 60) AND TO_DATE(CURRENT_DATE)
-- IMPALA
WHERE BOXAGR_EXTRACTION_DATE_P BETWEEN DATE_SUB(TO_DATE(NOW()), 60) AND TO_DATE(NOW())

-- DW_BUYBOX.BBOX_LOG_SELLER
SELECT
	BOXSEL_EXTRACTION_DATE, 
	BOXSEL_SELLER_ID, 
	BOXSEL_BRAND_NAME, 
	BOXSEL_LOW_PRICE, 
	BOXSEL_FIRST_AND_LOW_PRICE, 
	BOXSEL_QTY_PRODUCTS, 
	BOXSEL_FIRST, 
	BOXSEL_SECOND, 
	BOXSEL_THIRD, 
	BOXSEL_FOURTH, 
	BOXSEL_FIFTH, 
	BOXSEL_SIXTH, 
	BOXSEL_SEVENTH, 
	BOXSEL_EIGHT, 
	BOXSEL_NINETH, 
	BOXSEL_TENTH, 
	BOXSEL_OTHERS, 
	BOXSEL_TOTAL, 
	BOXSEL_EXTRACTION_DATE_P 
FROM DW_BUYBOX.BBOX_LOG_SELLER
-- HIVE
WHERE BOXSEL_EXTRACTION_DATE_P BETWEEN DATE_SUB(TO_DATE(CURRENT_DATE), 60) AND TO_DATE(CURRENT_DATE)
-- IMPALA
WHERE BOXSEL_EXTRACTION_DATE_P BETWEEN DATE_SUB(TO_DATE(NOW()), 60) AND TO_DATE(NOW()) 