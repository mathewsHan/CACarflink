package cn.cavehicle.test.utils;

import cn.cavehicle.utils.DateUtil;

public class DateUtilTest {

	public static void main(String[] args) {

		System.out.println(DateUtil.getCurrentDate());

		System.out.println(DateUtil.getCurrentDateTime());

		System.out.println(DateUtil.getCurrentDate("yyyyMMdd"));

		System.out.println(DateUtil.convertStringToDate("2022-05-14 11:20:08", DateUtil.DATE_TIME_FORMAT).getTime());

	}

}
