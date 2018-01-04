package com.genpact.housingloan.bigdata;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HousingLoan {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(new Configuration());
		
		job.setJarByClass(HousingLoan.class);
		
		job.setMapperClass(HousingLoanMapper.class);
		
		job.setReducerClass(HousingLoanReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BillRecordWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("/xsc/HousingLoan.txt"));
		
		FileOutputFormat.setOutputPath(job, new Path("/xsc/"+new Date().getTime()+""));
		
		job.waitForCompletion(true);
	}
}
