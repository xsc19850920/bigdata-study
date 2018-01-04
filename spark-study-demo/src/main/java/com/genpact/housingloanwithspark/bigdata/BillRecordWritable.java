package com.genpact.housingloanwithspark.bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.WritableComparable;

import com.alibaba.fastjson.JSONObject;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class BillRecordWritable implements WritableComparable<BillRecordWritable> {

	private String monthOnBill;
	private double monthPrincipal;
	private double monthCapital;
	private double monthInterest;
		
	public BillRecordWritable() {
	}
	
	public BillRecordWritable(String ...params) {
		this.monthOnBill = params[0];
		this.monthPrincipal = Double.parseDouble(params[1]);
		this.monthCapital = Double.parseDouble(params[2]);
		this.monthInterest = Double.parseDouble(params[3]);
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(monthOnBill);
		out.writeDouble(monthPrincipal);
		out.writeDouble(monthCapital);
		out.writeDouble(monthInterest);
	}
	public void readFields(DataInput in) throws IOException {
		monthOnBill = in.readUTF();
		monthPrincipal = in.readDouble();  
		monthCapital = in.readDouble();   
		monthInterest = in.readDouble();   
	}
	public int compareTo(BillRecordWritable o) {
		return 0;
	}
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
		return monthOnBill + "|" + monthPrincipal + "|" + monthCapital + "|" + monthInterest;
	}

	

	
}
