/*
 * Copyright 
 */

package com.genpact.adm.habse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

/**
 * 查询工具类
 * 
 * @author Sun
 * @version QueryUtil.java 2013-1-7 下午1:44:13
 */
@SuppressWarnings({"rawtypes","deprecation","unchecked"})
public class QueryUtil {
    public static List<HashMap> getRows(ResultScanner resultScanner) {
        List<HashMap> rows = new ArrayList<HashMap>();
        // hbase中一个result就是一行数据(包含多列), 这个直接与row key挂钩
        // 例如hbase的test_table中有3条数据
        // row1 cf:a r1a
        // row2 cf:a r2a
        // row1 cf:b r1b
        // 那么无条件查询(Scan)出来的结果(ResultScanner)中会包含2个Result
        // 一个Result(row1)包含a, b这2个列
        // 另一个Result(row2)只有a列
        for (Result result : resultScanner) {
            rows.add(getRow(result));
        }

        return rows;
    }

    public static List<HashMap> getRows(ResultScanner resultScanner,
            long offset, long limit) throws IOException {
        List<HashMap> rows = new ArrayList<HashMap>();

        // 跳过需要offset的数据
		if(offset > 0){
			resultScanner.next((int) offset);
		}

        if (limit > 0) {
            // 只取limit条数据
            Result[] limitResults = resultScanner.next((int) limit);
            for (Result result : limitResults) {
                rows.add(getRow(result));
            }
        } else {
            // 获取跳过需要offset的数据以后所有的数据
            return getRows(resultScanner);
        }

        return rows;
    }

	public static HashMap getRow(Result result) {
    	HashMap row = new HashMap();
        for (KeyValue kv : result.raw()) {
            String columnName = new String(kv.getFamily()) + ":" + new String(kv.getQualifier());
            String columnValue = new String(kv.getValue());
            row.put(columnName, columnValue);
        }
        return row;
    }
}
