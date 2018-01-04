package com.genpact.housingloanwithspark;

import org.junit.Test;

import com.genpact.housingloanwithspark.bigdata.LoanRecordWritable;


/**
 * Unit test for simple App.
 */

public class AppTest {
	@Test
	public void demo1(){
		LoanRecordWritable loan = new LoanRecordWritable();
		loan.setBills("1");
		loan.setInvest(1);
		loan.setName("1");
		loan.setStartMonth("1");
		loan.setYear(1);
		loan.setYearRate(1);
		System.out.println(loan.toString());
	}
}
