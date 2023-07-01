package cn.cavehicle.test.json;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * 解析json字符串生成对象
 * 解析嵌套JSON格式数据，思路：一层一横解析数据
 *      {}  ->  JSONObject
 *      []  ->  JSONArray
 */
public class JsonParsePlusTest {

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	private static class VehiclePlusJson {
		private int batteryAlarm;
		private int carMode;
		private double minVoltageBattery;
		private int chargeStatus;
		private String vin;
		private String probeTemperatures;
		private int chargeTemperatureProbeNum;
		private int childSystemNum;
	}

	public static void main(String[] args) {
		// json字符串
		String content = "{\n" +
			"    \"batteryAlarm\": 0,\n" +
			"    \"carMode\": 1,\n" +
			"    \"minVoltageBattery\": 3.89,\n" +
			"    \"chargeStatus\": 1,\n" +
			"    \"vin\": \"LS5A3CJC0JF890971\",\n" +
			"    \"nevChargeSystemTemperatureDtoList\": [{\n" +
			"        \"probeTemperatures\": [25, 23, 24, 21, 24, 21, 23, 21, 23, 21, 24, 21, 24, 21, 25, 21],\n" +
			"        \"chargeTemperatureProbeNum\": 16,\n" +
			"        \"childSystemNum\": 1\n" +
			"    }]\n" +
			"}" ;

		// 第1步、整体字符串解析 {} ，使用JSONObject对象
		JSONObject jsonObject = new JSONObject(content);
		int batteryAlarm = jsonObject.getInt("batteryAlarm");
		int carMode = jsonObject.getInt("carMode");
		double minVoltageBattery = jsonObject.getDouble("minVoltageBattery");
		int chargeStatus = jsonObject.getInt("chargeStatus");
		String vin = jsonObject.getString("vin");

		// 获取嵌套字符串值
		String nevChargeSystemTemperatureDtoList = jsonObject.get("nevChargeSystemTemperatureDtoList").toString();

		// 第2步、嵌套字符串解析 []  -> 使用JSONArray
		JSONArray jsonArray = new JSONArray(nevChargeSystemTemperatureDtoList);
		// todo: 获取数组中第一个元素的值
		String jsonStr = jsonArray.get(0).toString();

		// 第3步、再次解析JSON字符串 {}, 使用JSONObject
		JSONObject object = new JSONObject(jsonStr);
		// todo: 依据属性名称，获取字段的值
		int chargeTemperatureProbeNum = object.getInt("chargeTemperatureProbeNum");
		int childSystemNum = object.getInt("childSystemNum");
		String probeTemperatures = object.get("probeTemperatures").toString();  // 没有必要进步解析

		// 封装实体类对象
		VehiclePlusJson vehiclePlusJson = new VehiclePlusJson(
			batteryAlarm, carMode, minVoltageBattery, chargeStatus, vin, probeTemperatures, chargeTemperatureProbeNum, childSystemNum
		);
		System.out.println(vehiclePlusJson);
	}

}
