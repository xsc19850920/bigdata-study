package com.genpact.housingloanwithspark.bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;


public class HousingLoan {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final String pattern = "MM-dd-yyyy";
		SparkConf sparkConf = new SparkConf().setAppName("HouseLoanWithSpark").setMaster("spark://58.2.221.224:7077").setJars(new String[] { "/bigdata/spark/xsc/housingloanwithspark.jar" });

		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		
//		ctx.parallelize(list)

//		SQLContext sqlContext = new SQLContext(ctx);
		
		JavaRDD<String> lines = ctx.textFile("hdfs://58.2.221.224:9000/xsc/HousingLoan.txt", 1);
		JavaRDD<String[]> words = lines.flatMap(new FlatMapFunction<String, String[]>() {
			private static final long serialVersionUID = 1L;

			public Iterable<String[]> call(String t) throws Exception {
				List<String[]> list = new ArrayList<String[]>();
				list.add(SPACE.split(t));
				return list;
			}

		});
		
		

		JavaRDD<LoanRecordWritable> mapResult = words.map(new Function<String[], LoanRecordWritable>() {

			public LoanRecordWritable call(String[] v1) throws Exception {
				LoanRecordWritable loanRecord = new LoanRecordWritable(v1);
//				List<BillRecordWritable> list = new ArrayList<BillRecordWritable>();
				double monthRate = loanRecord.getYearRate() / 1200;
				int month = loanRecord.getYear() * 12;
				// 每月本息金额 = (贷款本金÷还款月数) + (贷款本金-已归还本金累计额)×月利率
				// 每月本金 = 贷款本金÷还款月数
				// 每月利息 = (贷款本金-已归还本金累计额)×月利率
				// double monthCapital = 0;
				double tmpCapital = 0;
				// double monthInterest = 0;
				Date startMonth = DateUtils.parseDate(loanRecord.getStartMonth(),new String[]{pattern});
				StringBuffer sb = new StringBuffer();
				for (int i = 1; i <= month; i++) {
					BillRecordWritable billRecord = new BillRecordWritable();
					billRecord.setMonthCapital((loanRecord.getInvest() / month) + (loanRecord.getInvest() - tmpCapital) * monthRate);
					billRecord.setMonthInterest((loanRecord.getInvest() - tmpCapital) * monthRate);
					tmpCapital = tmpCapital + (loanRecord.getInvest() / month);
					// System.out.println("第" + i + "月本息： " + monthCapital +
					// "，本金："
					// +
					// (loanRecord.getInvest() / month) + "，利息："
					// + monthInterest);
					billRecord.setMonthPrincipal(loanRecord.getInvest() / month);
					billRecord.setMonthOnBill(DateFormatUtils.format(DateUtils.addMonths(startMonth, i),pattern));
					sb.append(billRecord).append("!");
//					list.add(billRecord);
				}
				//loanRecord.setList(list);
				loanRecord.setBills(sb.toString());
				return loanRecord;
			}

		});
		
//		HUtils.getFs().deleteOnExit(new Path("hdfs://58.2.221.224:9000/xsc/sparkResult"));
		
		 mapResult.saveAsTextFile("hdfs://58.2.221.224:9000/xsc/sparkResult");

	/*	DataFrame schema = sqlContext.createDataFrame(mapResult, LoanRecordWritable.class);
		schema.registerTempTable("loanRecord");
		

		DataFrame teenagers = sqlContext.sql("SELECT name,invest,yearRate, year,startMonth,bills FROM loanRecord WHERE name = 42267");

		 JavaRDD<String> billList = teenagers.javaRDD().map(new Function<Row, String>() {
			public String call(Row row) {
				
// 				String name; 
//				double invest; 
//				double yearRate; 
//				int year;
//				String startMonth;
//				 String bills
				
				return "Name : "+row.getString(0)+ ", invest :" +row.getDouble(1)+ ", yearRate :" +row.getDouble(2)+ ", year :" +row.getInt(3)+ ", startMonth :" +row.getString(4)+ ", bills :" +row.getString(5)+System.lineSeparator();
//				return row.getString(0);
			}
		});
		 
		 
		billList.saveAsTextFile("hdfs://58.2.221.224:9000/xsc/" + new Date().getTime());*/

		ctx.close();
		ctx.stop();

		System.exit(0);

	}

}
