package cn.cavehicle.test.utils;

import cn.cavehicle.utils.RedisUtil;

public class RedisUtilTest {

	public static void main(String[] args) {

		// System.out.println(RedisUtil.getValue("wtw3q386"));

		String location = "{\n" +
			"  \"address\": \"上海市浦东新区北蔡镇莲振路莲振路-道路停车位\",\n" +
			"  \"city\": \"上海市\",\n" +
			"  \"country\": \"中国\",\n" +
			"  \"district\": \"浦东新区\",\n" +
			"  \"lat\": 31.16581,\n" +
			"  \"lng\": 121.56434,\n" +
			"  \"province\": \"上海市\",\n" +
			"  \"township\": \"北蔡镇\"\n" +
			"}" ;

		RedisUtil.setValue("wtw3q387", location);

	}

}
