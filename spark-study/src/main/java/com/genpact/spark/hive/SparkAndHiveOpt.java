package com.genpact.spark.hive;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;


/**
 * @author 710009498
 * 用hive查询hdfs
 * $SPARK_HOME/bin/spark-submit --class com.genpact.spark.hive.SparkAndHiveSearch  --executor-memory 2g --num-executors 3 file:////opt/hadoop/hadoop-2.6.0/share/hadoop/mapreduce/sparkstudy.jar 10-20-2015
 */
public class SparkAndHiveOpt {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("sparkstudy").setMaster("spark://58.2.221.224:7077").setJars(new String[] { "/opt/hadoop/hadoop-2.6.0/share/hadoop/mapreduce/sparkstudy.jar" });
		SparkContext sc = new SparkContext(sparkConf);
		HiveContext hiveContext = new HiveContext(sc);
		DataFrame dataFrame = hiveContext.read().json("hdfs://58.2.221.224:9000/xsc/sparkResult/part-00000");
		
		dataFrame.registerTempTable("billRecord");
		//private String monthOnBill;
		// double monthPrincipal;
		// double monthCapital;
		// double monthInterest;
		DataFrame teenagers = hiveContext.sql("SELECT monthOnBill,monthPrincipal,monthCapital,monthInterest FROM billRecord WHERE monthOnBill = '" + args[0] + "'");
		
		teenagers.javaRDD().saveAsTextFile("hdfs://58.2.221.224:9000/xsc/searchResult");
	}
}
