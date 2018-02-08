package com.genpact.housingloanwithspark.bigdata;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class HousingLoanMapper extends Mapper<LongWritable, Text, Text, BillRecordWritable> {
	@Override
	protected void map(LongWritable key,  Text value, Context context) throws IOException, InterruptedException {
		
		String pattern = "MM-dd-yyyy";
		String[] params = value.toString().split(" ");
		
		LoanRecordWritable loanRecord = new LoanRecordWritable(params);
		double monthRate = loanRecord.getYearRate() / 1200;
		int month = loanRecord.getYear() * 12;
		// 每月本息金额 = (贷款本金÷还款月数) + (贷款本金-已归还本金累计额)×月利率
		// 每月本金 = 贷款本金÷还款月数
		// 每月利息 = (贷款本金-已归还本金累计额)×月利率
		// double monthCapital = 0;
		double tmpCapital = 0;
		// double monthInterest = 0;
		Date startMonth = null;
		try {
			startMonth = DateUtils.parseDate(loanRecord.getStartMonth(), new String[]{pattern});
		} catch (ParseException e) {
			e.printStackTrace();
		}
		

		for (int i = 1; i <= month; i++) {
			BillRecordWritable billRecord = new BillRecordWritable();
			billRecord.setMonthCapital((loanRecord.getInvest() / month) + (loanRecord.getInvest() - tmpCapital) * monthRate);
			billRecord.setMonthInterest((loanRecord.getInvest() - tmpCapital) * monthRate);
			tmpCapital = tmpCapital + (loanRecord.getInvest() / month);
			// System.out.println("第" + i + "月本息： " + monthCapital + "，本金："
			// +
			// (loanRecord.getInvest() / month) + "，利息："
			// + monthInterest);
			billRecord.setMonthPrincipal(loanRecord.getInvest() / month);
			billRecord.setMonthOnBill(DateFormatUtils.format(DateUtils.addMonths(startMonth, i),pattern));
			context.write(new Text(loanRecord.getName()), billRecord);
		}
		
	}
}
