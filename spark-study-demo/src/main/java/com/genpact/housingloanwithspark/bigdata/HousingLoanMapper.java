package com.genpact.housingloanwithspark.bigdata;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.genpact.housingloanwithspark.utils.DateUtils;

public class HousingLoanMapper extends Mapper<LongWritable, Text, Text, BillRecordWritable> {
	@Override
	protected void map(LongWritable key,  Text value, Context context) throws IOException, InterruptedException {
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
		Date startMonth = DateUtils.parseDate(loanRecord.getStartMonth());
		

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
			billRecord.setMonthOnBill(DateUtils.format(org.apache.commons.lang.time.DateUtils.addMonths(startMonth, i)));
			context.write(new Text(loanRecord.getName()), billRecord);
		}
		
	}
}
