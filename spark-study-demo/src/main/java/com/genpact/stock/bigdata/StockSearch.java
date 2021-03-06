package com.genpact.stock.bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.alibaba.fastjson.JSONObject;
import com.genpact.constant.Constant;
import com.genpact.stock.bigdata.model.Stock;
import com.genpact.utils.HUtils;

public class StockSearch {


	/*
	 * SELECT STOCK_NAME, DAY, MIN(OPEN_PRICE) AS OPEN_PRICE,
	 * MAX(TRANSACTION_PRICE) AS CLOSE_PRICE, SUM(VOLUMN) AS TOTAL_VOLUMN,
	 * SUM(TRANSACTION_AMOUNT) AS TOTAL_TRANSACTION_AMOUNT FROM TRAN GROUP BY
	 * STOCK_NAME,SUBSTR(TIMESTAMP,1,8)
	 */
	public static void main(String[] args) {
		HUtils.deleteOnExit(Constant.HDFS_BASE_PATH + Constant.SEARCHRESULT);
		
		SparkConf sparkConf = new SparkConf().setAppName(Constant.STOCK_SEARCH_APP_NAME).setMaster(Constant.SPARK_HOST).setJars(new String[] { Constant.JAR_PATH });

		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		
		SQLContext sqlContext = new SQLContext(ctx);

		JavaRDD<String> lines = ctx.textFile(Constant.HDFS_BASE_PATH + Constant.STOCK_CALC_APP_NAME +"/" + Constant.RESULTFILE, 1);
		
		DataFrame schema = sqlContext.createDataFrame(lines.map((Function<String,Stock>) s -> JSONObject.parseObject(s, Stock.class)), Stock.class);
		schema.registerTempTable("stock");
        String sql  = "select "
		        		+ " stockName,"
		        		+ " timestamp,"
		        		+ " openPrice,"
		        		+ " transactionPrice,"
		        		+ " riseAndFall,"
		        		+ " volumn,"
		        		+ " transactionAmount"
		        		+ " from stock"
		        		+ " where timestamp = :timestamp";
        
        sql = sql.replace(":timestamp", args[0]);
		DataFrame teenagers = sqlContext.sql(sql);
		JavaRDD<String> stockList = teenagers.javaRDD().map((Function<Row, String>) r -> r.toString()+System.lineSeparator());
//		stockList.foreach(System.out::println);
		stockList.saveAsTextFile(Constant.HDFS_BASE_PATH + Constant.SEARCHRESULT);

		ctx.close();
		ctx.stop();

		System.exit(0);
	}
}
