/**
 * 
 */
package com.genpact.housingloan.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/**
 * Hadoop 工具类
 * 
 * @author fansy
 * @date 2015-5-28
 */
public class HUtils {


	private static Configuration conf = null;
	private static FileSystem fs = null;

	public static boolean flag = false; // get configuration from db or file
										// ,true : db,false:file


	public static Configuration getConf() {

		if (conf == null) {
			conf = new Configuration();
			// get configuration from db or file
			conf.setBoolean("mapreduce.app-submission.cross-platform", "true".equals(Utils.getKey("mapreduce.app-submission.cross-platform", flag)));// 配置使用跨平台提交任务
			conf.set("fs.defaultFS", Utils.getKey("fs.defaultFS", flag));// 指定namenode
			conf.set("mapreduce.framework.name", Utils.getKey("mapreduce.framework.name", flag)); // 指定使用yarn框架
			conf.set("yarn.resourcemanager.address", Utils.getKey("yarn.resourcemanager.address", flag)); // 指定resourcemanager
			conf.set("yarn.resourcemanager.scheduler.address", Utils.getKey("yarn.resourcemanager.scheduler.address", flag));// 指定资源分配器
			conf.set("mapreduce.jobhistory.address", Utils.getKey("mapreduce.jobhistory.address", flag));
		}

		return conf;
	}

	public static FileSystem getFs() {
		if (fs == null) {
			try {
				fs = FileSystem.get(getConf());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return fs;
	}

	/**
	 * 获取hdfs文件目录及其子文件夹信息
	 * 
	 * @param input
	 * @param recursive
	 * @return
	 * @throws IOException
	 */
	public static String getHdfsFiles(String input, boolean recursive)
			throws IOException {
		RemoteIterator<LocatedFileStatus> files = getFs().listFiles(
				new Path(input), recursive);
		StringBuffer buff = new StringBuffer();
		while (files.hasNext()) {
			buff.append(files.next().getPath().toString()).append("<br>");
		}

		return buff.toString();
	}

	

	/**
	 * 返回HDFS路径
	 * 
	 * @param url
	 * @return fs.defaultFs+url
	 */
	public static String getHDFSPath(String url) {
		return Utils.getKey("fs.defaultFS", flag) + url;
	}
	/**
	 * 获得Path路径
	 * 如果url包含hdfs:// ，那么flag使用true，否则flag使用false
	 * @param url
	 * @param flag
	 * @return
	 */
	public static Path getHDFSPath(String url,String flag) {
		if("true".equals(flag)){
			return new Path(url);
		}
		return new Path(getHDFSPath(url));
	}
	/**
	 * 上传本地文件到HFDS
	 * 
	 * @param localPath
	 * @param hdfsPath
	 * @return
	 */
	public static boolean upload(String localPath, String hdfsPath) {
		FileSystem fs = getFs();
		Path src = new Path(localPath);
		Path dst = new Path(hdfsPath);
		try {
			fs.copyFromLocalFile(src, dst);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	

}
