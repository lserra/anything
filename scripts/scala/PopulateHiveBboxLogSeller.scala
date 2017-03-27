object PopulateHiveBboxLogSeller {

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
		
		val conf = new SparkConf().setAppName("PopulateHiveBboxLogSeller")
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
		// create the SQL that will return the last date load from bbox_log_seller
		var str_sql_date = "select case when date_add(max(boxsel_extraction_date_p),1) >= substring(current_date,1,10) "
		str_sql_date += "then date_add(max(boxsel_extraction_date_p),0) "
		str_sql_date += "else date_add(max(boxsel_extraction_date_p)," + add_day.toString() + ") "
		str_sql_date += "end  as boxsel_extraction_date_p "
		str_sql_date += "from dw_buybox.bbox_log_seller"

		// execute the SQL with the last date load from bbox_log_seller
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
		var strSQL = "insert overwrite table dw_buybox.bbox_log_seller partition(boxsel_extraction_date_p) "
		strSQL += "select api.partition_date as boxsel_extraction_date "
		strSQL += ", api.sellerid as boxsel_seller_id "
		strSQL += ", api.load_subject as boxsel_brand_name "
		strSQL += ", sum(case when cast(api.normalizedlandedprice as float) = 1.0 then 1 else 0 end) as boxsel_low_price "
		strSQL += ", sum(case when cast(api.position as int) = 0 and cast(api.normalizedlandedprice as float) = 1.0 then 1 else 0 end) as boxsel_first_and_low_price "
		strSQL += ", count(distinct api.productid) as boxsel_qty_products "
		strSQL += ", sum(case when cast(api.position as int) = 0 then 1 else 0 end) as boxsel_first "
		strSQL += ", sum(case when cast(api.position as int) = 1 then 1 else 0 end) as boxsel_second "
		strSQL += ", sum(case when cast(api.position as int) = 2 then 1 else 0 end) as boxsel_third "
		strSQL += ", sum(case when cast(api.position as int) = 3 then 1 else 0 end) as boxsel_fourth "
		strSQL += ", sum(case when cast(api.position as int) = 4 then 1 else 0 end) as boxsel_fifth "
		strSQL += ", sum(case when cast(api.position as int) = 5 then 1 else 0 end) as boxsel_sixth "
		strSQL += ", sum(case when cast(api.position as int) = 6 then 1 else 0 end) as boxsel_seventh "
		strSQL += ", sum(case when cast(api.position as int) = 7 then 1 else 0 end) as boxsel_eight "
		strSQL += ", sum(case when cast(api.position as int) = 8 then 1 else 0 end) as boxsel_nineth "
		strSQL += ", sum(case when cast(api.position as int) = 9 then 1 else 0 end) as boxsel_tenth "
		strSQL += ", sum(case when cast(api.position as int) > 9 then 1 else 0 end) as boxsel_others "
		strSQL += ", count(*) as boxsel_total "
		strSQL += ", api.partition_date "
		strSQL += "from logs_front.log_buybox_api api "
		strSQL += "where api.partition_date = '" + dt_extraction.toString() + "'"
		strSQL += " group by "
		strSQL += "        api.sellerid "
		strSQL += "      , api.load_subject "
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