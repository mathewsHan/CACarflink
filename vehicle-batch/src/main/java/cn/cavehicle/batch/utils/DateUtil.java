package cn.cavehicle.batch.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 日期时间处理工具类：
 *      1. 获取当前日期
 *          yyyy-MM-dd\yyyyMMdd
 *      2. 获取当前日期时间
 *          yyyy-MM-dd HH:mm:ss
 *      3. 字符串转换日期时间
 *          String -> Date -> Long
 */
public class DateUtil {

	// 定义静态变量，日志时间格式
	public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss" ;
	public static final String DATE_FORMAT = "yyyy-MM-dd" ;

	/**
	 * 获取当前日期：yyyy-MM-dd
	 */
	public static String getCurrentDate(){
		// 构建SDF对象
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DATE_FORMAT);
		// 获取当前系统日期时间
		Date date = new Date() ;
		// 格式化，并返回
		return simpleDateFormat.format(date) ;
	}

	/**
	 * 获取当前日期时间：yyyy-MM-dd HH:mm:ss
	 */
	public static String getCurrentDateTime(){
		// 构建SDF对象
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DATE_TIME_FORMAT);
		// 获取当前系统日期时间
		Date date = new Date() ;
		// 格式化，并返回
		return simpleDateFormat.format(date) ;
	}

	/**
	 * 获取当前日期：yyyyMMdd
	 */
	public static String getCurrentDate(String format){
		// 构建SDF对象
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
		// 获取当前系统日期时间
		Date date = new Date() ;
		// 格式化，并返回
		return simpleDateFormat.format(date) ;
	}

	/**
	 * 将字符串类型日期时间转换为Date类型，指定格式
	 */
	public static Date convertStringToDate(String dateStr, String format){
		// 构建SDF对象
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);

		Date date = null ;
		try {
			date = simpleDateFormat.parse(dateStr);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date ;
	}

	/**
	 * 获取昨天日期，格式为：yyyy-MM-dd"
	 */
	public static String getYesterdayDate() {
		// 获取当天日期书剑
		Date date = new Date() ;
		// 获取Calendar对象
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		// 获取前一天
		calendar.add(Calendar.DATE, -1);
		Date yesterdayDate = calendar.getTime();
		// 格式化
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		String yesterday = format.format(yesterdayDate);
		// 返回结果
		return yesterday;
	}
}
