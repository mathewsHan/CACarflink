package cn.cavehicle.batch.utils;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置文件加载工具类，提供方法：依据key属性名称获取value值
 */
public class ConfigLoader {

	// 定义属性实例对象
	private final static Properties props = new Properties() ;

	// todo: 使用静态代码块，加载属性配置文件
	static {
		// 1. 使用类加载器读取文件为InputStream
		InputStream inputStream = ConfigLoader.class.getClassLoader().getResourceAsStream("config.properties");
		// 2. 将流中数据存储到Properties中
		try {
			props.load(inputStream);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 依据Key获取Value值，数据类型：String
	 */
	public static String get(String key){
		return props.getProperty(key, "");
	}

	/**
	 * 依据Key获取Value值，数据类型：Int
	 */
	public static int getInt(String key){
		String value = get(key);
		// 判断转换
		if(StringUtils.isNotEmpty(value)){
			return Integer.parseInt(value) ;
		}else{
			return -999999 ;
		}
	}

}
