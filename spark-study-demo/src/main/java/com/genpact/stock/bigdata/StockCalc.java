package com.genpact.stock.bigdata;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.genpact.constant.Constant;
import com.genpact.stock.bigdata.model.Stock;
import com.genpact.utils.HUtils;

public class StockCalc {

	/*
	 * SELECT STOCK_NAME, DAY, MIN(OPEN_PRICE) AS OPEN_PRICE,
	 * MAX(TRANSACTION_PRICE) AS CLOSE_PRICE, SUM(VOLUMN) AS TOTAL_VOLUMN,
	 * SUM(TRANSACTION_AMOUNT) AS TOTAL_TRANSACTION_AMOUNT FROM TRAN GROUP BY
	 * STOCK_NAME,SUBSTR(TIMESTAMP,1,8)
	 */
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		HUtils.deleteOnExit(Constant.HDFS_BASE_PATH + Constant.STOCK_CALC_APP_NAME);

		SparkConf sparkConf = new SparkConf().setAppName(Constant.STOCK_CALC_APP_NAME).setMaster(Constant.SPARK_HOST).setJars(new String[] { Constant.JAR_PATH });

		JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		JavaRDD<String> lines = ctx.textFile(Constant.HDFS_BASE_PATH + Constant.SOURCE_FILE, 1);
		JavaRDD<String> valueRdd = lines.map((Function<String, Stock>) s -> {
			if (!s.startsWith(Constant.STOPWORDS)) {
				return new Stock(s.split(Constant.FLAG));
			} else {
				return null;
			}
		}).filter((Function<Stock,Boolean>) s -> s != null).mapToPair((PairFunction<Stock, String, Stock>) t -> new Tuple2<String, Stock>(t.getStockName() + t.getTimestamp().substring(0, 8), t)).groupByKey().mapValues((Function<Iterable<Stock>, String>) list -> {
			Stock stock = new Stock();

			double minOpenPrice = 0d;
			double maxTransactionPrice = 0d;
			double sumVolumn = 0d;
			double sumTransactionAmount = 0d;
			for (Stock s : list) {
				if (StringUtils.isEmpty(stock.getStockName())) {
					stock.setStockName(s.getStockName());
					stock.setTimestamp(s.getTimestamp().substring(0, 8));
					minOpenPrice = s.getOpenPrice();
					maxTransactionPrice = s.getTransactionPrice();
				}
				if (minOpenPrice > s.getOpenPrice()) {
					minOpenPrice = s.getOpenPrice();
				}
				if (maxTransactionPrice < s.getTransactionPrice()) {
					maxTransactionPrice = s.getTransactionPrice();
				}
				sumVolumn += s.getVolumn();
				sumTransactionAmount += s.getTransactionAmount();
			}
			stock.setOpenPrice(minOpenPrice);
			stock.setTransactionPrice(maxTransactionPrice);
			stock.setVolumn(sumVolumn);
			stock.setTransactionAmount(sumTransactionAmount);
			return stock.toString();
		}).values();

		valueRdd.saveAsTextFile(Constant.HDFS_BASE_PATH + Constant.STOCK_CALC_APP_NAME);

		ctx.close();
		ctx.stop();

		System.exit(0);
	}
}
