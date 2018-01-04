package com.genpact.spark.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class Stream {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("streaming study").setMaster("spark://58.2.221.224:7077").setJars(new String[] { "/opt/hadoop/hadoop-2.6.0/share/hadoop/mapreduce/streamingstudy.jar" });
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaStreamingContext jsc = new JavaStreamingContext(sparkContext, Durations.seconds(1));
		JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 7777);
		JavaDStream<String> errorLines = lines.filter(new Function<String, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String line) throws Exception {
				return line.contains("error");
			}
		});
		
		errorLines.print();
		jsc.start();
		jsc.awaitTermination();
	}
}
