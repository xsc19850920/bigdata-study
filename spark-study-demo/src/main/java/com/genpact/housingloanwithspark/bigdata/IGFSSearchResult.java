package com.genpact.housingloanwithspark.bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.Ignition;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.alibaba.fastjson.JSONObject;

public class IGFSSearchResult {
	public static void main(String[] args) throws IllegalArgumentException, IOException {
		SparkConf sparkConf = new SparkConf().setAppName("HousingLoan").setMaster("spark://58.2.221.224:7077").setJars(new String[] { "/opt/hadoop/hadoop-2.6.0/share/hadoop/mapreduce/housingloanwithspark.jar" });

		JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		SQLContext sqlContext = new SQLContext(ctx);

		JavaRDD<String> lines = ctx.textFile("igfs://58.2.221.224:10500/xsc/sparkResult/part-00000", 1);
		
		JavaRDD<LoanRecordWritable> result = lines.flatMap(new FlatMapFunction<String, LoanRecordWritable>() {

			public Iterable<LoanRecordWritable> call(String t) throws Exception {
				LoanRecordWritable loadRecord = JSONObject.parseObject(t, LoanRecordWritable.class);
				List<LoanRecordWritable> list = new ArrayList<LoanRecordWritable>();
				list.add(loadRecord);
				return list;
			}
		});
		
		

			DataFrame schema = sqlContext.createDataFrame(result, LoanRecordWritable.class);
			schema.registerTempTable("loanRecord");
			

			DataFrame teenagers = sqlContext.sql("SELECT name,invest,yearRate, year,startMonth,bills FROM loanRecord WHERE name = '"+args[0]+"'");

			 JavaRDD<String> billList = teenagers.javaRDD().map(new Function<Row, String>() {
				public String call(Row row) {
					
//	 				String name; 
//					double invest; 
//					double yearRate; 
//					int year;
//					String startMonth;
//					 String bills
					
					return "Name : "+row.getString(0)+ ", invest :" +row.getDouble(1)+ ", yearRate :" +row.getDouble(2)+ ", year :" +row.getInt(3)+ ", startMonth :" +row.getString(4)+ ", bills :" +row.getString(5)+System.lineSeparator();
//					return row.getString(0);
				}
			});
//			String IGNITE_HOME = "/opt/hadoop/apache-ignite-hadoop-1.7.0-bin/";
//				// if you want get the cache data from server ignite
//				// The ClientMode must bu set true
//			Ignition.setClientMode(true);
//			Ignite ignite = Ignition.start(IGNITE_HOME + "config/default-config.xml");
//			IgniteFileSystem fs = ignite.fileSystem("igfs");
//			IgfsPath dir = new IgfsPath("/xsc/IGFSSearchResult");
//			if(fs.exists(dir)){
//				fs.delete(dir, true);
//			}
			billList.saveAsTextFile("igfs://58.2.221.224:10500/xsc/IGFSSearchResult");

			ctx.close();
			ctx.stop();
			

			System.exit(0);
		
		
	}
}
