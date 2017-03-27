-- DASHBOARD  PRICING NEW

SELECT * 
FROM PRICING.PRC_PRODUCT_LIST_HIST
WHERE PRCPRD_TIME_P = '2017-02-21'

select * from datalake_rdw.rdw_flash_qg where to_date(dt_pedido) = '2017-02-20' limit 50
select * from pricing.prc_margin_cost_hist limit 50;
select * from pricing.prc_stock_hist limit 50;
select * from pricing.prc_product_list_hist limit 50;