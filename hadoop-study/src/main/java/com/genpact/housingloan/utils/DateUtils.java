package com.genpact.housingloan.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {
	private static SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yyyy");
	public static Date parseDate(String source){
		try {
			return sdf.parse(source);
		} catch (ParseException e) {
			return null;
		}
	}
	
	public static String format(Date date){
		return sdf.format(date);
	}
}
