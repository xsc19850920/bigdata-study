package com.genpact.adm.habse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


@SuppressWarnings({"rawtypes","deprecation"})
public class HbaseQuery {
	public static HBaseConfiguration HBASECONFIG = null; 
	public static HBaseAdmin HBASEADMIN = null; 
    
	/**
	 * 初始化
	 * @param quorum
	 * @param port
	 * @throws MasterNotRunningException
	 * @throws ZooKeeperConnectionException
	 * @throws IOException
	 */
	public static void init(String quorum,String port) {
    	Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", quorum);  
        config.set("hbase.zookeeper.property.clientPort", port);
        HBASECONFIG = new HBaseConfiguration(config);
		try {
			HBASEADMIN = new HBaseAdmin(HBASECONFIG);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
		init("58.2.219.221", "2181");
//		HBASEADMIN.createNamespace(NamespaceDescriptor.create("TEST").build());
//		deleteTable("TEST:TEST");
//		 creatTable("TEST:TEST", new String[]{"people"});
		
//		Map<String,String[]> columns = new HashMap<String, String[]>();
//		columns.put("people", new String[]{"name","age","sex","height"});
//		Map<String,String[]> values = new HashMap<String, String[]>();
//		values.put("people", new String[]{"王森2","32","男","187"});
//		addData("2", "TEST:TEST", columns, values);
		List<HashMap> query = query("TEST:TEST",new String[]{"people:name","people:age","people:sex","people:height"},0,5);
		for (int i = 0; i < query.size(); i++) {
			System.out.println(query.get(i));
		}
//		System.out.println("---------------------------------------------------------------------");
//		TableName[] listTableNames = HBASEADMIN.listTableNames();
//		for (int i = 0; i < listTableNames.length; i++) {
//			System.out.println(listTableNames[i].getNameAsString());
//		}
//		System.out.println("---------------------------------------------------------------------");
	}

	public static List<HashMap> query(String tableName,String[] columns, long start, long limit) {
		List<HashMap> rows = new ArrayList<HashMap>();
		try {
			HTable table = getHTable(tableName);
			Scan scan = new Scan();
			for (int i = 0; i < columns.length; i++) {
				String [] f_q = columns[i].split(":");
				String family = f_q[0];
				String qualifier = f_q[1];
				scan.addColumn(family.getBytes(), qualifier.getBytes());
			}
			ResultScanner ResultScannerFilterList = table.getScanner(scan);
			rows = QueryUtil.getRows(ResultScannerFilterList, start, limit);
			ResultScannerFilterList.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return rows;
	}
	
	/**
	 * 创建表
	 * @param tableName
	 * @param family
	 * @throws Exception
	 */
	public static void creatTable(String tableName, String[] family) throws Exception {
		if (HBASEADMIN.tableExists(tableName)) {
			HTable table = getHTable(tableName);
			HTableDescriptor desc = new HTableDescriptor(table.getTableDescriptor());
	        for (int i = 0; i < family.length; i++) {
	        	if(!desc.hasFamily(family[i].getBytes())){
	        		desc.addFamily(new HColumnDescriptor(family[i]));
	        	}
	        }
	        HBASEADMIN.modifyTable(tableName, desc);
	        System.out.println("modify table Success!");
		}else{
			HTableDescriptor desc = getHTableDescriptor(tableName);
	        for (int i = 0; i < family.length; i++) {
	            desc.addFamily(new HColumnDescriptor(family[i]));
	        }
	        HBASEADMIN.createTable(desc);
	        System.out.println("create table Success!");
		}
    }
	
	public static void addData(String rowKey, String tableName, Map<String,String[]> columns, Map<String,String[]> values) throws Exception{
		Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
        HTable table = getHTable(tableName);
        for(String key : columns.keySet()){
    		String [] cs = columns.get(key);
    		String [] vs = values.get(key);
    		for (int j = 0; j < cs.length; j++) {
                put.add(Bytes.toBytes(key), Bytes.toBytes(cs[j]), Bytes.toBytes(vs[j]));
            }
        }
        table.put(put);
	}
	
	/**
	 * 根据rwokey查询
	 * @param tableName
	 * @param rowKey
	 * @return
	 * @throws IOException
	 */
	public static HashMap getResult(String tableName, String rowKey) throws Exception{
		Get get = new Get(Bytes.toBytes(rowKey));
		HTable table = getHTable(tableName);
		Result result = table.get(get);
		return  QueryUtil.getRow(result);
	}
	
	/**
	 * 删除全部的列
	 * @param tableName
	 * @param rowKey
	 * @throws IOException
	 */
	public static void deleteAllColumn(String tableName, String rowKey) throws Exception {
		HTable table = getHTable(tableName);
		Delete deleteAll = new Delete(Bytes.toBytes(rowKey));
		table.delete(deleteAll);
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
		String falilyName, String columnName) throws IOException {
		HTable table = getHTable(tableName);
		Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
		deleteColumn.deleteColumns(Bytes.toBytes(falilyName), Bytes.toBytes(columnName));
		table.delete(deleteColumn);
		System.out.println(falilyName + ":" + columnName + "is deleted!");
	}
	 
	/**
	 * 删除表
	 * @param tableName
	 * @throws IOException
	 */
	public static void deleteTable(String tableName) throws Exception {
        HBASEADMIN.disableTable(tableName);
        HBASEADMIN.deleteTable(tableName);
        System.out.println(tableName + "is deleted!");
    }
	
	public static HTable getHTable(String tableName) throws IOException{
    	return new HTable(HBASECONFIG, tableName);
    }
    
    public static HTableDescriptor getHTableDescriptor(String tableName) throws IOException{
    	return new HTableDescriptor(tableName);
    }

}
