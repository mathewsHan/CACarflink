package cn.cavehicle.streaming.function.flat;

import cn.cavehicle.utils.DateUtil;
import cn.cavehicle.entity.ElectricFenceDimension;
import cn.cavehicle.entity.ElectricFenceModel;
import cn.cavehicle.entity.VehicleDataPartObj;
import cn.cavehicle.utils.DistanceCaculateUtil;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 车辆数据流与维度数据流进行connect连接以后，对其中数据进行处理，类似批处理中大表数据（车辆数据流）与小表数据（维表：map）关联计算
 */
public class ElectricFenceDimensionConnectFunction
	implements CoFlatMapFunction<VehicleDataPartObj, Map<String, List<ElectricFenceDimension>>, ElectricFenceModel> {

	// 定义变量，接收广播流中数据，就是一个HashMap集合
	private Map<String, List<ElectricFenceDimension>> hashMap = new HashMap<>() ;

	// 处理大表数据流中每条数据
	@Override
	public void flatMap1(VehicleDataPartObj vehicleDataPartObj, Collector<ElectricFenceModel> out) throws Exception {
		// todo 1: 获取车架号vin值
		String vinValue = vehicleDataPartObj.getVin();

		// todo 2: 依据vin获取对应电子围栏信息（vin是否监控车辆）
		List<ElectricFenceDimension> list = hashMap.getOrDefault(vinValue, null);

		// todo 3: 如果车辆为监控车辆, 进一步判断在电子围栏内，还是电子围栏外
		if(null != list && list.size() > 0){
			// todo 4: 循环遍历每条电子围栏数据，进行距离计算和判断
			for (ElectricFenceDimension dimension : list) {
				// 4-1. 判断车辆数据gpsTime时间，是否在电子围栏监控有效时间范围内，进行下一步
				String gpsTime = vehicleDataPartObj.getGpsTime();
				long gpsTimeStamp = DateUtil.convertStringToDate(gpsTime, DateUtil.DATE_TIME_FORMAT).getTime();
				if(gpsTimeStamp >= dimension.getStartTime().getTime() && gpsTimeStamp <= dimension.getEndTime().getTime()){
					// 4-2. 获取车辆数据经纬度，判断是否有值，有值继续处理
					if(vehicleDataPartObj.getLng() != -999999D && vehicleDataPartObj.getLat() != -999999D){
						// 4-3. 计算距离，依据车辆数据中经纬度和电子围栏中心点经纬
						Double distance = DistanceCaculateUtil.getDistance(
							vehicleDataPartObj.getLat(), vehicleDataPartObj.getLng(),
							dimension.getLatitude(), dimension.getLongitude()
						);
						double distanceKm = distance / 1000D ;
						System.out.println("车辆数据位置与电子围栏中心点距离：" + distanceKm + ", 电子围栏半径： " + dimension.getRadius() );

						// 4-4. 比较距离与电子围栏半径
						int fenceStatus = 0; // 默认值，电子围栏外
						if(distanceKm <= dimension.getRadius()){
							fenceStatus = 1 ; // 围栏内
						}

						// todo 5: 创建监控数据对象，设置相关属性
						ElectricFenceModel fenceModel = new ElectricFenceModel() ;
						fenceModel.setVin(vinValue);
						fenceModel.setFenceStatus(fenceStatus);

						fenceModel.setGpsTime(gpsTime);
						fenceModel.setLat(vehicleDataPartObj.getLat());
						fenceModel.setLng(vehicleDataPartObj.getLng());
						fenceModel.setTerminalTimestamp(vehicleDataPartObj.getTerminalTimeStamp());

						fenceModel.setEleId(dimension.getId());
						fenceModel.setEleName(dimension.getName());
						fenceModel.setAddress(dimension.getAddress());
						fenceModel.setLatitude(dimension.getLatitude());
						fenceModel.setLongitude(dimension.getLongitude());
						fenceModel.setRadius(dimension.getRadius());

						// todo 6: 输出结果数据
						out.collect(fenceModel);
					}else{
						System.out.println("此条车辆数据中经纬度不符合要求，属于异常值： 经度 = " + vehicleDataPartObj.getLng() + ", 纬度 = " + vehicleDataPartObj.getLat());
					}
				}else{
					System.out.println("此条车辆数据GPS时间不在电子围栏监控时间之内: " + dimension.getStartTime() + " ~ " + dimension.getEndTime() + "........");
				}
			}
		}else {
			System.out.println("车辆<" + vinValue + ">: 不是监控车辆.................................");
		}
	}

	// 处理小表数据流中每条数据：map集合
	@Override
	public void flatMap2(Map<String, List<ElectricFenceDimension>> value, Collector<ElectricFenceModel> out) throws Exception {
		// 直接赋值
		hashMap = value ;
	}
}
