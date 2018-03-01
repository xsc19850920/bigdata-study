package com.genpact.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import parquet.org.slf4j.Logger;
import parquet.org.slf4j.LoggerFactory;
import scala.Tuple2;

import com.genpact.constant.Constant;

public class HbaseStudy {
	private static Logger LOGGER = LoggerFactory.getLogger(HbaseStudy.class);
	private static String[] fq = { "people:name", "people:sex", "people:age", "people:height" };
	public static void main(String[] args) throws IOException {
		// step 1 修改host 地址： c:\windows\system32\drivers\etc

		String jarPath = "/bigdata/spark/xsc/hbasestudy.jar";
		SparkConf sparkConf = new SparkConf().setAppName(Constant.HBASE_APP_NAME).setMaster(Constant.SPARK_HOST).setJars(new String[] { jarPath, System.getenv("jars") });
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		JavaRDD<Map<String, String>> rdd = null;
		
		if(ArrayUtils.isNotEmpty(args) && "FromUtils".equals(args[0])){
				rdd = getRDDFromHbase(jsc);
		}else{
			rdd = getRDDFromUtils(jsc);
		}
		// HbaseUtils.creatTable(Constant.RESULT_TABLE_NAME, new
		// String[]{"people"});
		Map<String, String[]> columns = new HashMap<String, String[]>();
		columns.put("people", new String[] { "total_age", "total_height" });
		Map<String, String[]> values = new HashMap<String, String[]>();
		final Map<String,String> map = new HashMap<String, String>();
		rdd.collect().stream().filter(m->MapUtils.isNotEmpty(m)).forEach(m -> {
			if(StringUtils.isNotEmpty(m.get("total_age"))){
				map.put("total_age", m.get("total_age"));
			}
			
			if(StringUtils.isNotEmpty(m.get("total_height"))){
				map.put("total_height", m.get("total_height"));
			}
		});
		values.put("people", new String[] {map.get("total_age") ,map.get("total_height") });
		HbaseUtils.addData("1", Constant.RESULT_TABLE_NAME, columns, values);
		// HbaseUtils.deleteTable(Constant.RESULT_TABLE_NAME);
	}

	private static JavaRDD<Map<String, String>> getRDDFromUtils(JavaSparkContext jsc) {
		List<HbaseModel> list = HbaseUtils.queryByFamilyAndQualifierArray(Constant.TABLE_NAME, fq);
		JavaRDD<Map<String, String>> rdd = jsc.parallelize(list).filter((Function<HbaseModel, Boolean>) v1 -> v1.getQualifier().equals("age") || v1.getQualifier().equals("height")).groupBy((Function<HbaseModel, String>) v1 -> {
			if (v1.getQualifier().equals("age")) {
				return "total_age";
			} else if (v1.getQualifier().equals("height")) {
				return "total_height";
			} else {
				return null;
			}
		}).map((Function<Tuple2<String, Iterable<HbaseModel>>, Map<String, String>>) v1 -> {
			Map<String, String> map = new HashMap<String, String>();
			Iterator<HbaseModel> it = v1._2.iterator();
			long total = 0;
			while (it.hasNext()) {
				HbaseModel model = it.next();
				total = total + Long.parseLong(model.getValue());
			}
			map.put(v1._1, String.valueOf(total));
			System.out.println(map.toString());
			return map;
		});

		return rdd;
	}

	private static JavaRDD<Map<String, String>> getRDDFromHbase(JavaSparkContext jsc) {

		HbaseUtils.CONFIG.addResource("/usr/lib/hbase/conf/hbase-site.xml");
		HbaseUtils.CONFIG.set(TableInputFormat.INPUT_TABLE, Constant.TABLE_NAME);
		String[] fq = { "people:name", "people:sex", "people:age", "people:height" };
		try {
			ClientProtos.Scan proto = ProtobufUtil.toScan(HbaseUtils.getScan(fq));
			String scanToString = Base64.encodeBytes(proto.toByteArray());
			HbaseUtils.CONFIG.set(TableInputFormat.SCAN, scanToString);
		} catch (IOException e) {
			e.printStackTrace();
		}

		JavaRDD<Map<String, String>> rdd = jsc.newAPIHadoopRDD(HbaseUtils.CONFIG, TableInputFormat.class, ImmutableBytesWritable.class, Result.class).map((Function<Tuple2<ImmutableBytesWritable, Result>, List<HbaseModel>>) s -> HbaseUtils.getRow(s._2)).flatMap((FlatMapFunction<List<HbaseModel>, HbaseModel>) s -> s).filter((Function<HbaseModel, Boolean>) v1 -> v1.getQualifier().equals("age") || v1.getQualifier().equals("height")).groupBy(( Function<HbaseModel, String>) v1 -> {
				if (v1.getQualifier().equals("age")) {
					return "total_age";
				} else if (v1.getQualifier().equals("height")) {
					return "total_height";
				} else {
					return null;
				}
		}).map((Function<Tuple2<String, Iterable<HbaseModel>>, Map<String, String>>) v1 -> {
				Map<String, String> map = new HashMap<String, String>();
				Iterator<HbaseModel> it = v1._2.iterator();
				long total = 0;
				while (it.hasNext()) {
					HbaseModel model = it.next();
					total = total + Long.parseLong(model.getValue());
				}
				map.put(v1._1, String.valueOf(total));
				System.out.println(map.toString());
				return map;
		});

		return rdd;
	}
}
