package cn.cavehicle.test.json;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

/**
 * 解析嵌套JSON格式数据，思路：一层一横解析数据
 *      {}  ->  JSONObject
 *      []  ->  JSONArray
 *      todo： 由于车辆数据特殊性，可能JSON字符串中，某些字段数据丢失，如果依据属性名称获取值，将会抛出异常
 *      todo： 方法 -> 先将解析属性放入map集合，再从map集合依据属性获取值，如果没有值，返回默认值。
 */
public class JsonParsePlusOptimizedTest {

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

		// 第1步、整体字符串解析 {} -> Map 集合
		Map<String, Object> resultMap = jsonStrToMap(content);
		// todo: 依据属性name到map集合汇总获取value值，如果没有值，给予默认值
		int batteryAlarm = Integer.parseInt(resultMap.getOrDefault("batteryAlarm", -999999).toString());
		int carMode = Integer.parseInt(resultMap.getOrDefault("carMode", -999999).toString());
		double minVoltageBattery = Double.parseDouble(resultMap.getOrDefault("minVoltageBattery", -999999D).toString());
		int chargeStatus = Integer.parseInt(resultMap.getOrDefault("chargeStatus", -999999).toString());
		String vin = resultMap.getOrDefault("vin", "").toString();
		// 获取嵌套字符串值
		String nevChargeSystemTemperatureDtoList = resultMap.getOrDefault("nevChargeSystemTemperatureDtoList", "[]").toString();
		System.out.println(nevChargeSystemTemperatureDtoList);

		// 第2步、解析json字符串： [{}]  -> List[Map] 集合
		List<Map<String, Object>> resultList = jsonStrToList(nevChargeSystemTemperatureDtoList) ;
		// todo: 由于列表中只有一条数据，直接获取值
		String probeTemperatures = "" ;
		int chargeTemperatureProbeNum = -999999 ;
		int childSystemNum = -999999;
		if(resultList.size() > 0){
			Map<String, Object> objectMap = resultList.get(0);
			// 依据属性名称，获取值，如果没有，给予默认值
			probeTemperatures = objectMap.getOrDefault("probeTemperatures", "").toString();
			chargeTemperatureProbeNum = Integer.parseInt(objectMap.getOrDefault("chargeTemperatureProbeNum", -999999).toString());
			childSystemNum = Integer.parseInt(objectMap.getOrDefault("childSystemNum", -999999).toString());
		}

		// 封装实体类对象
		VehiclePlusJson vehiclePlusJson = new VehiclePlusJson(
			batteryAlarm, carMode, minVoltageBattery, chargeStatus, vin, probeTemperatures, chargeTemperatureProbeNum, childSystemNum
		);
		System.out.println(vehiclePlusJson);
	}

	/**
	 * 解析JSON字符串为list列表，列表中{} 字符串，再次解析封装到Map集合
	 */
	private static List<Map<String, Object>> jsonStrToList(String content) {
		// a. 构建JSONArray对象
		JSONArray jsonArray = new JSONArray(content) ;
		// b. 循环遍历获取列表中每个值
		int length = jsonArray.length();  // 列表元素格式
		List<Map<String, Object>> list = new ArrayList<>();
		for(int index = 0; index < length; index ++){
			// b-1. 依据下标获取列表中的值
			String jsonStr = jsonArray.get(index).toString();
			// b-2. 再次解析{} 字符串 -> Map集合
			Map<String, Object> jsonMap = jsonStrToMap(jsonStr);
			// b-3. 添加到列表
			list.add(jsonMap);
		}
		// c. 返回列表数据即可
		return list;
	}

	/**
	 * 解析JSON字符串{} ，将属性name和对应值value，存储到map集合中
	 */
	private static Map<String, Object> jsonStrToMap(String content) {
		// a. 构建JSONObject对象
		JSONObject jsonObject = new JSONObject(content);
		// b. 获取所有name值： keys
		Set<String> keySet = jsonObject.keySet();
		// c. 遍历Set集合，依据name获取value值，并且放入map集合
		Map<String, Object> map = new HashMap<>();
		for (String key : keySet) {
			Object value = jsonObject.get(key);
			map.put(key, value) ;
		}
		// d. 返回map集合
		return map;
	}

}
