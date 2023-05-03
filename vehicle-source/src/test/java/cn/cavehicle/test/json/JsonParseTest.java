package cn.cavehicle.test.json;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.json.JSONObject;

/**
 * 解析JSON字符串（简易JSON数据，没有嵌套），使用org.json库解析
 *      1. {}   -> 构建JSONObject对象
 *          依据属性名称获取值
 *      2. []  -> 构建JSONArray对象
 *          使用get指定索引下标获取值
 */
public class JsonParseTest {

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	private static class VehicleJson {
		private Integer batteryAlarm;
		private Integer carMode;
		private Double minVoltageBattery;
		private Integer chargeStatus;
		private String vin;
	}

	public static void main(String[] args) {
		// json字符串：
		String content = "{\n" +
			"    \"batteryAlarm\": 0,\n" +
			"    \"carMode\": 1,\n" +
			"    \"minVoltageBattery\": 3.89,\n" +
			"    \"chargeStatus\": 1,\n" +
			"    \"vin\": \"LS5A3CJC0JF890971\"\n" +
			"}" ;

		// 1. 构建JSONObject对象 -> todo: 可以认为时map集合
		JSONObject jsonObject = new JSONObject(content) ;

		// 2. 依据json字符串中属性名称获取值
		int batteryAlarm = jsonObject.getInt("batteryAlarm");
		int carMode = jsonObject.getInt("carMode");
		double minVoltageBattery = jsonObject.getDouble("minVoltageBattery");
		int chargeStatus = jsonObject.getInt("chargeStatus");
		String vin = jsonObject.getString("vin");

		// 3. 封装对象
		VehicleJson vehicleJson = new VehicleJson(batteryAlarm, carMode, minVoltageBattery, chargeStatus, vin);
		System.out.println(vehicleJson);
	}


}
