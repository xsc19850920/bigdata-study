package com.genpact.spark;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.genpact.spark.pojo.BillRecord;
import com.genpact.spark.pojo.LoanRecord;
import com.genpact.spark.utils.DateUtils;

// $SPARK_HOME/bin/spark-submit --class com.genpact.spark.OptRDD  --executor-memory 2g --num-executors 3 file:////opt/hadoop/hadoop-2.6.0/share/hadoop/mapreduce/sparkstudy.jar
public class OptRDD {

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("sparkstudy").setMaster("spark://58.2.221.224:7077").setJars(new String[] { "/opt/hadoop/hadoop-2.6.0/share/hadoop/mapreduce/sparkstudy.jar" });

		JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		JavaRDD<String> lines = ctx.textFile("hdfs://58.2.221.224:9000/xsc/HousingLoan.txt", 1);

		JavaRDD<BillRecord> list = lines.flatMap(new FlatMapFunction<String, BillRecord>() {

			private static final long serialVersionUID = 1L;

			public Iterable<BillRecord> call(String t) throws Exception {
				Collection<BillRecord> list = new ArrayList<BillRecord>();

				String[] paramArr = SPACE.split(t);
				LoanRecord loanRecord = new LoanRecord(paramArr);
				double monthRate = loanRecord.getYearRate() / 1200;
				int month = loanRecord.getYear() * 12;
				// 每月本息金额 = (贷款本金÷还款月数) + (贷款本金-已归还本金累计额)×月利率
				// 每月本金 = 贷款本金÷还款月数
				// 每月利息 = (贷款本金-已归还本金累计额)×月利率
				// double monthCapital = 0;
				double tmpCapital = 0;
				// double monthInterest = 0;
				Date startMonth = DateUtils.parseDate(loanRecord.getStartMonth());
				StringBuffer sb = new StringBuffer();

				for (int i = 1; i <= month; i++) {
					BillRecord billRecord = new BillRecord();
					billRecord.setMonthCapital((loanRecord.getInvest() / month) + (loanRecord.getInvest() - tmpCapital) * monthRate);
					billRecord.setMonthInterest((loanRecord.getInvest() - tmpCapital) * monthRate);
					tmpCapital = tmpCapital + (loanRecord.getInvest() / month);
					// System.out.println("第" + i + "月本息： " + monthCapital +
					// "，本金："
					// +
					// (loanRecord.getInvest() / month) + "，利息："
					// + monthInterest);
					billRecord.setMonthPrincipal(loanRecord.getInvest() / month);
					billRecord.setMonthOnBill(DateUtils.format(org.apache.commons.lang.time.DateUtils.addMonths(startMonth, i)));
					sb.append(billRecord).append("!");
					list.add(billRecord);
				}
				return list;
			}

		});
		list.saveAsTextFile("hdfs://58.2.221.224:9000/xsc/sparkResult");
		list.cache();
		ctx.close();
		System.exit(0);
	}
}
