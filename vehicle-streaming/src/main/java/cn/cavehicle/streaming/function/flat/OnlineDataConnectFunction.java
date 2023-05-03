package cn.cavehicle.streaming.function.flat;

import cn.cavehicle.entity.OnlineDataModel;
import cn.cavehicle.entity.VehicleDimension;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 对窗口计算预警数据和正常数据与车辆基本信息数据广播，进行拉宽操作，最后保存到MySQL表中
 */
public class OnlineDataConnectFunction
	implements CoFlatMapFunction<OnlineDataModel, Map<String, VehicleDimension>, OnlineDataModel> {

	// 定义变量，接收广播流中数据
	private Map<String, VehicleDimension> hashMap = new HashMap<>() ;

	// 窗口数据流，相当于大表数据流
	@Override
	public void flatMap1(OnlineDataModel onlineDataModel, Collector<OnlineDataModel> out) throws Exception {
		// 1. 获取车架号vin
		String vin = onlineDataModel.getVin();
		// 2. 获取车辆维度数据
		VehicleDimension vehicleDimension = hashMap.getOrDefault(vin, null);
		// 3. 如果有维度数据，设置窗口数据属性值
		if(null != vehicleDimension){
			onlineDataModel.setSeriesName(vehicleDimension.getSeriesName());
			onlineDataModel.setModelName(vehicleDimension.getModelName());
			onlineDataModel.setLiveTime(vehicleDimension.getLiveTime());
			onlineDataModel.setSalesDate(vehicleDimension.getSalesDate());
			onlineDataModel.setCarType(vehicleDimension.getCarType());
		}
		// 4. 输出数据
		out.collect(onlineDataModel);
	}

	// 广播数据流，车辆维度数据，存储到Map集合中
	@Override
	public void flatMap2(Map<String, VehicleDimension> value, Collector<OnlineDataModel> out) throws Exception {
		// 直接将广播流中数据赋值即可
		hashMap = value ;
	}
}