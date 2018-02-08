package com.genpact.stock.bigdata.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.lang.NumberUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.WritableComparable;

import com.alibaba.fastjson.JSONObject;

@SuppressWarnings("deprecation")
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Stock implements WritableComparable<Stock>,Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String stockName;
	private String timestamp;
	private double openPrice;
	private double transactionPrice;
	private double riseAndFall;
	private double volumn;
	private double transactionAmount;
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(stockName);
		out.writeUTF(timestamp);
		out.writeDouble(openPrice);         
		out.writeDouble(transactionPrice);  
		out.writeDouble(riseAndFall);       
		out.writeDouble(volumn);            
		out.writeDouble(transactionAmount); 
	}
	public void readFields(DataInput in) throws IOException {
		this.stockName = in.readUTF();
		this.timestamp = in.readUTF();
		this.openPrice = in.readDouble();
		this.transactionPrice = in.readDouble();
		this.riseAndFall = in.readDouble();
		this.volumn = in.readDouble();
		this.transactionAmount = in.readDouble();
	}
	
	public Stock() {
	}
	
	

	public Stock(String[] args) {
		this.stockName = args[0];
		this.timestamp = args[1];
		this.openPrice = NumberUtils.createDouble(args[2]);
		this.transactionPrice = NumberUtils.createDouble(args[3]);
		this.riseAndFall = NumberUtils.createDouble(args[4]);
		this.volumn = NumberUtils.createDouble(args[5]);
		this.transactionAmount = NumberUtils.createDouble(args[6]);
	}
	public int compareTo(Stock o) {
		return 0;
	}
	public String getStockName() {
		return stockName;
	}
	public void setStockName(String stockName) {
		this.stockName = stockName;
	}
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	public double getOpenPrice() {
		return openPrice;
	}
	public void setOpenPrice(double openPrice) {
		this.openPrice = openPrice;
	}
	public double getTransactionPrice() {
		return transactionPrice;
	}
	public void setTransactionPrice(double transactionPrice) {
		this.transactionPrice = transactionPrice;
	}
	public double getRiseAndFall() {
		return riseAndFall;
	}
	public void setRiseAndFall(double riseAndFall) {
		this.riseAndFall = riseAndFall;
	}
	public double getVolumn() {
		return volumn;
	}
	public void setVolumn(double volumn) {
		this.volumn = volumn;
	}
	public double getTransactionAmount() {
		return transactionAmount;
	}
	public void setTransactionAmount(double transactionAmount) {
		this.transactionAmount = transactionAmount;
	}
	@Override
	public String toString() {
		return JSONObject.toJSONString(this);
	}
	
}
