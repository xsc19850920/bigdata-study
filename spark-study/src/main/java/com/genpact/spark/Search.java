package com.genpact.spark;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.alibaba.fastjson.JSONObject;
import com.genpact.spark.pojo.BillRecord;

//	$SPARK_HOME/bin/spark-submit --class com.genpact.spark.Search  --executor-memory 2g --num-executors 3 file:////opt/hadoop/hadoop-2.6.0/share/hadoop/mapreduce/sparkstudy.jar 10-20-2015
public class Search {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("sparkstudy").setMaster("spark://58.2.221.224:7077").setJars(new String[] { "/opt/hadoop/hadoop-2.6.0/share/hadoop/mapreduce/sparkstudy.jar" });

		JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		SQLContext sqlContext = new SQLContext(ctx);

		JavaRDD<String> lines = ctx.textFile("hdfs://58.2.221.224:9000/xsc/sparkResult/part-00000", 1);

		JavaRDD<BillRecord> result = lines.flatMap(new FlatMapFunction<String, BillRecord>() {
			private static final long serialVersionUID = 1L;

			public Iterable<BillRecord> call(String t) throws Exception {
				BillRecord obj = JSONObject.parseObject(t, BillRecord.class);
				Collection<BillRecord> list = new ArrayList<BillRecord>();
				list.add(obj);
				return list;
			}

		});

		result.cache();
		
//		JavaPairRDD<String, BillRecord> pair = result.mapToPair(new PairFunction<BillRecord, String, BillRecord>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Tuple2<String, BillRecord> call(BillRecord t) throws Exception {
//				return null;
//			}
//			
//		});
//		
//		pair.reduceByKey(new Function2<BillRecord, BillRecord, BillRecord>() {
//		});
//		JavaPairRDD<String, Iterable<BillRecord>> groupResult = pair.groupByKey();

		DataFrame schema = sqlContext.createDataFrame(result, BillRecord.class);
		schema.registerTempTable("billRecord");
		DataFrame teenagers = sqlContext.sql("SELECT * FROM billRecord WHERE monthOnBill = '" + args[0] + "'");
		teenagers.javaRDD().saveAsTextFile("hdfs://58.2.221.224:9000/xsc/searchResult");
		
		
		
		
	}
}
