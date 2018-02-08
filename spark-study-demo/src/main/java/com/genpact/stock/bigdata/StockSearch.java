package com.genpact.stock.bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.alibaba.fastjson.JSONObject;
import com.genpact.housingloanwithspark.utils.HUtils;
import com.genpact.stock.bigdata.model.Stock;

public class StockSearch {
	private static final String JAR_PATH = "/bigdata/spark/xsc/stockcalc.jar";
	private static final String CALC_APP_NAME = "StockCalc";
	private static final String APP_NAME = "StockSearch";

	private static final String SPARK_HOST = "spark://58.2.221.224:7077";
	private static final String HDFS_BASE_PATH = "hdfs://58.2.221.224:9000/xsc/";
	private static final String RESULTFILE = "part-00000";
	private static final String SEARCHRESULT = "searchResult";

	/*
	 * SELECT STOCK_NAME, DAY, MIN(OPEN_PRICE) AS OPEN_PRICE,
	 * MAX(TRANSACTION_PRICE) AS CLOSE_PRICE, SUM(VOLUMN) AS TOTAL_VOLUMN,
	 * SUM(TRANSACTION_AMOUNT) AS TOTAL_TRANSACTION_AMOUNT FROM TRAN GROUP BY
	 * STOCK_NAME,SUBSTR(TIMESTAMP,1,8)
	 */
	public static void main(String[] args) {
		HUtils.deleteOnExit(HDFS_BASE_PATH + SEARCHRESULT);
		
		SparkConf sparkConf = new SparkConf().setAppName(APP_NAME).setMaster(SPARK_HOST).setJars(new String[] { JAR_PATH });

		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		
		SQLContext sqlContext = new SQLContext(ctx);

		JavaRDD<String> lines = ctx.textFile(HDFS_BASE_PATH + CALC_APP_NAME +"/" + RESULTFILE, 1);
		
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
		stockList.foreach(System.out::println);
		stockList.saveAsTextFile(HDFS_BASE_PATH + SEARCHRESULT);

		ctx.close();
		ctx.stop();

		System.exit(0);
	}
}
