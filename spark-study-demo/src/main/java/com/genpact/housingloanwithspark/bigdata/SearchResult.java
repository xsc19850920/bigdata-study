package com.genpact.housingloanwithspark.bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.alibaba.fastjson.JSONObject;
import com.genpact.housingloanwithspark.utils.HUtils;

public class SearchResult {
	public static void main(String[] args) throws IllegalArgumentException, IOException {
		SparkConf sparkConf = new SparkConf().setAppName("HousingLoan").setMaster("spark://58.2.221.224:7077").setJars(new String[] { "/opt/hadoop/hadoop-2.6.0/share/hadoop/mapreduce/housingloanwithspark.jar" });

		JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		SQLContext sqlContext = new SQLContext(ctx);

		JavaRDD<String> lines = ctx.textFile("hdfs://58.2.221.224:9000/xsc/sparkResult/part-00000", 1);
		
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
			 
			HUtils.getFs().deleteOnExit(new Path("hdfs://58.2.221.224:9000/xsc/searchResult"));
			billList.saveAsTextFile("hdfs://58.2.221.224:9000/xsc/searchResult");

			ctx.close();
			ctx.stop();

			System.exit(0);
		
		
	}
}
