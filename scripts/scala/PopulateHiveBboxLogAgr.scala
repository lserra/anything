object PopulateHiveBboxLogAgr {

	def main(args: Array[String]) : Unit = {

		import org.apache.spark.sql.hive.HiveContext
		import org.apache.spark.SparkConf
		import org.apache.spark.SparkContext
		
		// to use this only in the notebook Zeppelin
		// %spark
		// start the Spark context
		
		// to use this only in the notebook Zeppelin
		// create a Spark context object
		// z.sc

		// to use this only in the notebook Zeppelin
		// val sqlContext = new HiveContext(z.sc)
		
		val conf = new SparkConf().setAppName("PopulateHiveBboxLogAgr")
        val sc = new SparkContext(conf)		
        val sqlContext = new HiveContext(sc)

		// enable the following properties to make it work
		sqlContext.setConf("hive.exec.dynamic.partition", "true") 
		sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict") 

		// flow control to select the correct values
		var add_day = 1
		var dt_extraction: String = "2016-01-01"
		var num_records: Long = 0

		do {
		// create the SQL that will return the last date load from bbox_log_agr
		var str_sql_date = "select case when date_add(max(boxagr_extraction_date_p),1) >= substring(current_date,1,10) "
		str_sql_date += "then date_add(max(boxagr_extraction_date_p),0) "
		str_sql_date += "else date_add(max(boxagr_extraction_date_p)," + add_day.toString() + ") "
		str_sql_date += "end  as boxagr_extraction_date_p "
		str_sql_date += "from dw_buybox.bbox_log_agr"

		// execute the SQL with the last date loaded from bbox_log_agr
		var result_date = sqlContext.sql(str_sql_date)

		// create a data cache to accelerate the search
		// result_date.cache()

		// get the last date and assign a constant
		dt_extraction = result_date.first().getDate(0).toString()
		println(dt_extraction)

		// create the SQL that will verify if exists some log for this date
		var str_sql_load = "select count(api.partition_date) as boxsel_extraction_date "
		str_sql_load += "from logs_front.log_buybox_api api "
		str_sql_load += "where api.partition_date = '" + dt_extraction + "'"
		str_sql_load += " group by api.partition_date"

		// execute the SQL that verify if exists some log for this date
		var result_load = sqlContext.sql(str_sql_load)

		// create a data cache to accelerate the search
		// result_load.cache()

		// get the total records and assign a variable
		num_records = result_load.distinct().count()
		if( num_records == 1 ){
		        num_records = result_load.collect()(0)(0).asInstanceOf[Long]         
		        println(num_records)
				} 
		else {
		        println(num_records)  
			}

		// plus one to the variable control that will be used into SQL that will return the last date load 
		// from bbox_log_seller
		add_day += 1
		} 
		while(num_records.asInstanceOf[Long] == 0)

		// create a SQL that will return a DataFrame with the data values to insert into the Hive table
		var strSQL = "insert overwrite table dw_buybox.bbox_log_agr partition(boxagr_extraction_date_p) "
		strSQL += "select api.partition_date as boxagr_extraction_date"
		strSQL += ", api.load_subject   as boxagr_brand_name"
		strSQL += ", count(distinct api.sellerid) as boxagr_sellers_qty"
		strSQL += ", count(distinct api.productid) as boxagr_products_qty"
		strSQL += ", count(distinct case when api.sellerid = '00776574000660' then api.productid else null end) as boxagr_xp_products_1p_3p"
		strSQL += ", sum(case when cast(api.position as int) = 0   then 1 else 0 end) as boxagr_total_cases"
		strSQL += ", sum(case when api.sellerid = '00776574000660' then 1 else 0 end) as boxagr_bbox_1p_3p"
		strSQL += ", sum(case when cast(api.position as int) = 0   then 1 else 0 end) - sum(case when api.sellerid = '00776574000660' then 1 else 0 end) as boxagr_bbox_3p"
		strSQL += ", sum(case when api.sellerid = '00776574000660'       and cast(api.position as int) = 0 then 1 else 0 end) as boxagr_1p_winner"
		strSQL += ", sum(case when not (api.sellerid = '00776574000660') and cast(api.position as int) = 0 then 1 else 0 end) as boxagr_3p_winner"
		strSQL += ", sum(case when api.sellerid = '00776574000660'       and cast(api.position as int) = 0 then 1 else 0 end) as boxagr_1p_mix_winner"
		strSQL += ", sum(case when api.sellerid = '00776574000660' then 1 else 0 end) - sum(case when api.sellerid = '00776574000660' and cast(api.position as int) = 0 then 1 else 0 end) as boxagr_3p_mix_winner"
		strSQL += ", sum(case when api.sellerid = '00776574000660' and cast(api.normalizedlandedprice as int) = 1 and cast(api.position as int) = 0 then 1 else 0 end) as boxagr_1p_cheap_winner"             
		strSQL += ", sum(case when not (api.sellerid = '00776574000660') and cast(api.normalizedlandedprice as int) = 1 and cast(api.position as int) = 0 then 1 else 0 end) as boxagr_3p_cheap_winner"             
		strSQL += ", sum(case when cast(api.normalizedlandedprice as int) = 1 and cast(api.position as int) = 0 then 1 else 0 end) as boxagr_cheap_winner"           
		strSQL += ", count(*) as boxagr_records_qty"
		strSQL += ", api.partition_date "
		strSQL += "from logs_front.log_buybox_api api "
		strSQL += "where api.partition_date = '" + dt_extraction.toString() + "'"
		strSQL += " group by "
		strSQL += "      api.load_subject"
		strSQL += "      , api.partition_date"

		// println(strSQL)

		// save the DataFrame into the Hive table
		val result_ins = sqlContext.sql(strSQL)
		// result_ins.write.mode("overwrite").saveAsTable("dw_buybox.bbox_log_seller") 
		// result_ins.write.insertInto("dw_buybox.bbox_log_seller")

		// send a message with the total records inserted
		println("Job finished and " + num_records.toString() + " records has been inserted.")

		}
	}
