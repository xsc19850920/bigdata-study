package com.genpact.constant;

public class Constant {
	public static final String JAR_PATH = "/bigdata/spark/xsc/stockcalc.jar";
	public static final String SPARK_HOST = "spark://58.2.221.224:7077";
	public static final String HDFS_BASE_PATH = "hdfs://58.2.221.224:9000/xsc/";
	public static final String RESULTFILE = "part-00000";

	public static final String SEARCHRESULT = "searchResult";
	
	public static final String SOURCE_FILE = "stockcalc.txt";
	
	public static final String FLAG = ",";
	public static final String STOPWORDS = "STOCK_NAME";
	
	public static final String STOCK_CALC_APP_NAME = "StockCalc";
	public static final String STOCK_SEARCH_APP_NAME = "StockSearch";

	
	
	//add for hbase
	public static final String HBASE_APP_NAME = "HBASE_STUDY";
	public static final String HBASE_ZOOKEEPER_QUORUM = "58.2.219.221";
	public static final String HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT="2181";
	public static final String TABLE_NAME = "TEST:TEST";
	public static final String RESULT_TABLE_NAME = "TEST:RESULT";

}
