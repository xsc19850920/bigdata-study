/**
 * 
 */
package com.genpact.housingloan.utils;

//import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;

/**
 * 工具类
 * @author fansy
 * @date 2015-6-8
 */
public class Utils {

	
	
	private static ResourceBundle resb = null;
	
	
	/**
	 * 初始化登录表数据
	 */
	public static String getKey(String key,boolean dbOrFile){
		
		if(resb==null){
			Locale locale = new Locale("zh", "CN"); 
            resb = ResourceBundle.getBundle("util", locale); 
		}
        return resb.getString(key);
	}
	
	
}
