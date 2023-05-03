package cn.cavehicle.streaming.function.flat;

import cn.cavehicle.entity.ElectricFenceModel;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 定义函数，对connect连接流中数据进行处理，确定驶出电子围栏数据对应结果表中id和inTime
 */
public class ElectricFenceConnectFunction
	implements CoFlatMapFunction<ElectricFenceModel, Map<String, Tuple2<Long, String>>, ElectricFenceModel> {

	// 定义变量，存储广播的数据
	private Map<String, Tuple2<Long, String>> resultMap = new HashMap<>() ;

	// 窗口结果数据流处理
	@Override
	public void flatMap1(ElectricFenceModel model, Collector<ElectricFenceModel> out) throws Exception {
		// a. 获取计算结果数据vin和eleId
		String key = model.getVin() + "_" + model.getEleId() ;
		// b. 从map集合中获取对应元组数据
		Tuple2<Long, String> tuple = resultMap.getOrDefault(key, null);
		// c. 判断并赋值
		if(null != tuple && 0 == model.getFenceStatus()){
			// 设置数据
			model.setId(tuple.f0);
			model.setInTime(tuple.f1);
		}
		// d. 输出数据
		out.collect(model);
	}

	// 广播流： MySQL数据库中电子围栏分析结果表数据流，封装在Map集合中，直接获取即可
	@Override
	public void flatMap2(Map<String, Tuple2<Long, String>> value, Collector<ElectricFenceModel> out) throws Exception {
		resultMap = value ;
	}

}
