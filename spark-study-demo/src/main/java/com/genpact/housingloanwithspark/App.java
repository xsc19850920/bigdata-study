package com.genpact.housingloanwithspark;

import java.util.ArrayList;
import java.util.List;

import com.genpact.housingloanwithspark.bigdata.BillRecordWritable;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
      List<BillRecordWritable> list =new ArrayList<BillRecordWritable>();
      BillRecordWritable b =new BillRecordWritable();
      list.add(b);
      System.out.println(list.get(0).getClass());
    }
}
