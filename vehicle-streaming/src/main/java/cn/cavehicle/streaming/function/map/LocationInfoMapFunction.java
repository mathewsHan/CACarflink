package cn.cavehicle.streaming.function.map;

import cn.cavehicle.entity.VehicleDataPartObj;
import cn.cavehicle.entity.VehicleLocationModel;
import cn.cavehicle.utils.GeoHashUtil;
import cn.cavehicle.utils.RedisUtil;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * 自定义函数，实现从Redis获取地理位置信息数据
 *      todo: 依据经纬生成GeoHash值, 获取对应的地理位置数据, 解析JSON为实体类, 设置到车辆数据中
 */
public class LocationInfoMapFunction extends RichMapFunction<VehicleDataPartObj, VehicleDataPartObj> {
	@Override
	public VehicleDataPartObj map(VehicleDataPartObj vehicleDataPartObj) throws Exception {
		// 1. 获取经纬度
		Double lng = vehicleDataPartObj.getLng();
		Double lat = vehicleDataPartObj.getLat();
		// 2. 经纬度生成GeoHash值
		String geoHash = GeoHashUtil.encode(lat, lng);
		// 3. 请求Redis数据库获取位置
		String location = RedisUtil.getValue(geoHash);
		// 4. 如果获取到位置信息数据，解析json字符串
		if(null != location){
			System.out.println("请求Redis获取位置信息: " + location);
			// 解析json字符串
			VehicleLocationModel vehicleLocationModel = JSON.parseObject(location, VehicleLocationModel.class);
			// 5. 设置属性值
			vehicleDataPartObj.setCounty(vehicleLocationModel.getCountry());
			vehicleDataPartObj.setProvince(vehicleLocationModel.getProvince());

			String city = vehicleLocationModel.getCity();
			if(StringUtils.isEmpty(city)){
				city = vehicleLocationModel.getProvince();
			}
			vehicleDataPartObj.setCity(city);

			vehicleDataPartObj.setAddress(vehicleLocationModel.getAddress());
		}
		// 6. 返回对象
		return vehicleDataPartObj;
	}
}
