package com.genpact.spark.pojo;

import java.io.Serializable;

import com.alibaba.fastjson.JSONObject;

public class BillRecord implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String monthOnBill;
	private double monthPrincipal;
	private double monthCapital;
	private double monthInterest;
		
	public BillRecord() {
	}
	
	public BillRecord(String ...params) {
		this.monthOnBill = params[0];
		this.monthPrincipal = Double.parseDouble(params[1]);
		this.monthCapital = Double.parseDouble(params[2]);
		this.monthInterest = Double.parseDouble(params[3]);
	}

//	public void write(DataOutput out) throws IOException {
//		out.writeUTF(monthOnBill);
//		out.writeDouble(monthPrincipal);
//		out.writeDouble(monthCapital);
//		out.writeDouble(monthInterest);
//	}
//	public void readFields(DataInput in) throws IOException {
//		monthOnBill = in.readUTF();
//		monthPrincipal = in.readDouble();  
//		monthCapital = in.readDouble();   
//		monthInterest = in.readDouble();   
//	}
//	
	public String getMonthOnBill() {
		return monthOnBill;
	}
	public void setMonthOnBill(String monthOnBill) {
		this.monthOnBill = monthOnBill;
	}
	public double getMonthPrincipal() {
		return monthPrincipal;
	}
	public void setMonthPrincipal(double monthPrincipal) {
		this.monthPrincipal = monthPrincipal;
	}
	public double getMonthCapital() {
		return monthCapital;
	}
	public void setMonthCapital(double monthCapital) {
		this.monthCapital = monthCapital;
	}
	public double getMonthInterest() {
		return monthInterest;
	}
	public void setMonthInterest(double monthInterest) {
		this.monthInterest = monthInterest;
	}

	@Override
	public String toString() {
		return JSONObject.toJSONString(this);
	}

	

	
}
