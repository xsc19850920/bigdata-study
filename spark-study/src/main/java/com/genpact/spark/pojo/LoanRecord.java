package com.genpact.spark.pojo;

import java.io.Serializable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.alibaba.fastjson.JSONObject;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class LoanRecord implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	// 贷款人
	private String name;
	// 贷款总金额
	private double invest;
	// 年华利率
	private double yearRate;
	// 贷款年限
	private int year;
	// 开始计息的月份
	private String startMonth;
	
	private String bills;

	public LoanRecord() {
	}

	public LoanRecord(String... params) {
		this.name = params[0];
		this.invest = Double.parseDouble(params[1]);
		this.yearRate = Double.parseDouble(params[2]);
		this.year = Integer.parseInt(params[3]);
		this.startMonth = params[4];
	}

//	public void write(DataOutput out) throws IOException {
//		out.writeUTF(name);
//		out.writeDouble(invest);
//		out.writeDouble(yearRate);
//		out.writeInt(year);
//		out.writeUTF(startMonth);
//		out.writeUTF(bills);
//	}
//
//	public void readFields(DataInput in) throws IOException {
//		name = in.readUTF();
//		invest = in.readDouble();
//		yearRate = in.readDouble();
//		year = in.readInt();
//		startMonth = in.readUTF();
//		bills = in.readUTF();
//	}
//
//	public int compareTo(LoanRecordWritable o) {
//		return 0;
//	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public double getInvest() {
		return invest;
	}

	public void setInvest(double invest) {
		this.invest = invest;
	}

	public double getYearRate() {
		return yearRate;
	}

	public void setYearRate(double yearRate) {
		this.yearRate = yearRate;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public String getStartMonth() {
		return startMonth;
	}

	public void setStartMonth(String startMonth) {
		this.startMonth = startMonth;
	}
	
	

	

	public String getBills() {
		return bills;
	}

	public void setBills(String bills) {
		this.bills = bills;
	}

	@Override
	public String toString() {
		return JSONObject.toJSONString(this);
	}

	


	
}
