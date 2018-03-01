package com.genpact.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;

import parquet.org.slf4j.Logger;
import parquet.org.slf4j.LoggerFactory;

import com.genpact.constant.Constant;

public class HbaseUtils {
	private static Logger LOGGER = LoggerFactory.getLogger(HbaseUtils.class);
	public static Configuration CONFIG = null;
	public static Admin HBASEADMIN = null; 
	public static Connection CONNECTION = null;
	
	static{
		CONFIG = new Configuration();
		CONFIG.set("hbase.zookeeper.quorum", Constant.HBASE_ZOOKEEPER_QUORUM);
		CONFIG.set("hbase.zookeeper.property.clientPort", Constant.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT);
		CONFIG = HBaseConfiguration.create(CONFIG);
		try {
			CONNECTION = ConnectionFactory.createConnection(CONFIG);
			HBASEADMIN = CONNECTION.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
//	public static Configuration getConfig(String tableName) {
//		init(tableName);
//		return config;
//	}

//	public static void init(String tableName){
//		config = new Configuration();
//		config.set("hbase.zookeeper.quorum", Constant.HBASE_ZOOKEEPER_QUORUM);
//		config.set("hbase.zookeeper.property.clientPort", Constant.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT);
//		config.set(TableInputFormat.INPUT_TABLE, tableName);
//		config = HBaseConfiguration.create(config);
//	}
	
	public static List<HbaseModel> getRows(ResultScanner resultScanner) {
		List<HbaseModel> rows = new ArrayList<HbaseModel>();
		// hbase中一个result就是一行数据(包含多列), 这个直接与row key挂钩
		// 例如hbase的test_table中有3条数据
		// row1 cf:a r1a
		// row2 cf:a r2a
		// row1 cf:b r1b
		// 那么无条件查询(Scan)出来的结果(ResultScanner)中会包含2个Result
		// 一个Result(row1)包含a, b这2个列
		// 另一个Result(row2)只有a列
		for (Result result : resultScanner) {
			rows.addAll(getRow(result));
		}

		return rows;
	}

	public static List<HbaseModel> getRows(ResultScanner resultScanner, long offset, long limit) {
		List<HbaseModel> rows = new ArrayList<HbaseModel>();

		// 跳过需要offset的数据
		if (offset > 0) {
			try {
				resultScanner.next((int) offset);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		if (limit > 0) {
			// 只取limit条数据
			Result[] limitResults = null;
			try {
				limitResults = resultScanner.next((int) limit);
			} catch (IOException e) {
				e.printStackTrace();
			}
			for (Result result : limitResults) {
				rows.addAll(getRow(result));
			}
		} else {
			// 获取跳过需要offset的数据以后所有的数据
			return getRows(resultScanner);
		}

		return rows;
	}

	public static List<HbaseModel> getRow(Result result) {
		List<HbaseModel> list = new ArrayList<HbaseModel>();
		Cell[] cells = result.rawCells();
		for (Cell cell : cells) {
			// cell.getRowArray() 得到数据的byte数组
			// cell.getRowOffset() 得到rowkey在数组中的索引下标
			// cell.getRowLength() 得到rowkey的长度
			// 将rowkey从数组中截取出来并转化为String类型
			String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
			String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
			String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
			String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
			String tags = Bytes.toString(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength());
			String str = String.format("Tags:%s , Qualifier:%s , Family:%s , Row:%s , Value:%s", tags, qualifier, family, row, value);
			System.out.println(str);
			LOGGER.info(str);
			HbaseModel model = new HbaseModel(tags, qualifier, family, row, value);
			list.add(model);
		}
		return list;
	}

	public static Table getTable(String tableName) {
//		init(tableName);
		Table table = null;
		try {
			
			table = CONNECTION.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return table;
	}
	
	public static ResultScanner getResultScanner(String[] fq,Table table){
		ResultScanner resultScanner = null;
		Scan scan = getScan(fq);
		try {
			 resultScanner = table.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return resultScanner;
	}

	public static Scan getScan(String[] fq) {
		Scan scan = new Scan();
		for (String entry : fq) {
			String[] f_q = entry.split(":");
			scan.addColumn(f_q[0].getBytes(), f_q[1].getBytes());
		}
		return scan;
	}
	
	
	public static List<HbaseModel> queryByFamilyAndQualifierArray(String tableName,String[] fq){
		Table table = getTable(tableName);
		ResultScanner resultScanner = getResultScanner(fq, table);
		return getRows(resultScanner);
	}
	
	public static List<HbaseModel> queryByFamilyAndQualifierArray(String tableName,String[] fq,int offset,int limit){
		Table table = getTable(tableName);
		ResultScanner resultScanner = getResultScanner(fq, table);
		return getRows(resultScanner,offset,limit);
	}
	
	
	
	/**
	 * 创建表
	 * @param tableName
	 * @param family
	 * @throws Exception
	 */
	public static void creatTable(String tableName, String[] family)  {
		try {
			if (HBASEADMIN.tableExists(TableName.valueOf(tableName))) {
			    Table table = getTable(tableName);
				HTableDescriptor desc = new HTableDescriptor(table.getTableDescriptor());
		        for (int i = 0; i < family.length; i++) {
		        	if(!desc.hasFamily(family[i].getBytes())){
		        		desc.addFamily(new HColumnDescriptor(family[i]));
		        	}
		        }
		        HBASEADMIN.modifyTable(TableName.valueOf(tableName), desc);
		        System.out.println("modify table Success!");
			}else{
				HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
		        for (int i = 0; i < family.length; i++) {
		            desc.addFamily(new HColumnDescriptor(family[i]));
		        }
		        HBASEADMIN.createTable(desc);
		        System.out.println("create table Success!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
    }
	
	
	/**
	 * 删除表
	 * @param tableName
	 * @throws IOException
	 */
	public static void deleteTable(String tableName)  {
        try {
			HBASEADMIN.disableTable(TableName.valueOf(tableName));
			HBASEADMIN.deleteTable(TableName.valueOf(tableName));
			System.out.println(tableName + "is deleted!");
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
	
	
	/**
	 * 删除全部的列
	 * @param tableName
	 * @param rowKey
	 * @throws IOException
	 */
	public static void deleteAllColumn(String tableName, String rowKey) {
		Table table = getTable(tableName);
		Delete deleteAll = new Delete(Bytes.toBytes(rowKey));
		try {
			table.delete(deleteAll);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("all columns are deleted!");
	}
	
	/**
	 * 删除指定的列
	 * @param tableName
	 * @param rowKey
	 * @param falilyName
	 * @param columnName
	 * @throws IOException
	 */
	public static void deleteColumn(String tableName, String rowKey,
		String falilyName, String columnName)  {
		Table table = getTable(tableName);
		Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
		deleteColumn.addColumns(Bytes.toBytes(falilyName), Bytes.toBytes(columnName));
		try {
			table.delete(deleteColumn);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(falilyName + ":" + columnName + "is deleted!");
	}
	
	/**
	 * 方法名:addData
	 * 描    述:TODO
	 * 返回值:void
	 * 参    数:@param rowKey
	 * 参    数:@param tableName
	 * 参    数:@param columns
	 * 参    数:@param values
	 * 参    数:@throws Exception
	 * 作    者:710009498
	 * 时    间:Feb 28, 2018 2:09:48 PM
	 */
	public static void addData(String rowKey, String tableName, Map<String,String[]> columns, Map<String,String[]> values) {
		Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
        Table table = getTable(tableName);
        for(String key : columns.keySet()){
    		String [] cs = columns.get(key);
    		String [] vs = values.get(key);
    		for (int j = 0; j < cs.length; j++) {
                put.addColumn(Bytes.toBytes(key), Bytes.toBytes(cs[j]), Bytes.toBytes(vs[j]));
            }
        }
        try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static void main(String[] args) {
//		HBASEADMIN.createNamespace(NamespaceDescriptor.create("TEST").build());
//		deleteTable("TEST:TEST");
//		 creatTable("TEST:TEST", new String[]{"people"});
		
		Map<String, String[]> columns = new HashMap<String, String[]>();
		columns.put("people", new String[] { "total_age", "total_height" });
		Map<String, String[]> values = new HashMap<String, String[]>();
		values.put("people", new String[] {64+"",64+"" });
		HbaseUtils.addData("1", Constant.RESULT_TABLE_NAME, columns, values);
//		 String[] fq = { "people:name", "people:sex", "people:age",
//		 "people:height" };
//		queryByFamilyAndQualifierArray(Constant.TABLE_NAME, fq);
		
		
		deleteAllColumn(Constant.RESULT_TABLE_NAME, "people");
	}
	
}
