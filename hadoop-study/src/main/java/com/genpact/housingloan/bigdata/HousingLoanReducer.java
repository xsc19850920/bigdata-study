package com.genpact.housingloan.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class HousingLoanReducer extends Reducer<Text, BillRecordWritable, Text, Text> {

	@Override
	protected void reduce(Text k2, Iterable<BillRecordWritable> v2s, Context context) throws IOException, InterruptedException {
		for (BillRecordWritable obj : v2s) {
			context.write(k2, new Text(obj.toString()));
		}
		
	}

}
