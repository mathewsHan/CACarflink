package cn.cavehicle.streaming.function.window;

import cn.cavehicle.entity.ElectricFenceModel;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.LinkedList;

/**
 * 窗口函数实现：对窗口中监控车辆数据计算，首先确定监控车辆在围栏内还是外（投票法），其次确定驶入还是驶出围栏（以前状态）。
 */
public class ElectricFenceMonitorWindowFunction
	extends RichWindowFunction<ElectricFenceModel, ElectricFenceModel, Tuple2<String, Integer>, TimeWindow> {

	// a. 定义状态，存储每个监控车辆针对电子围栏状态
	private MapState<String, Integer> fenceState = null ;
	// 定义状态，存储监控车辆驶入电子围栏时间
	private MapState<String, String> inTimeState = null ;

	@Override
	public void open(Configuration parameters) throws Exception {
		ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

		// b. 创建状态描述符对象
		MapStateDescriptor<String, Integer> fenceStateDescriptor = new MapStateDescriptor<String, Integer>(
			"fenceState", String.class, Integer.class
		);
		// c. 设置状态TTL
		StateTtlConfig stateTtlConfig = StateTtlConfig
			.newBuilder(Time.milliseconds(parameterTool.getLong("state.ttl.millionseconds", 86400000L)))
			.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
			.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
			.build();
		fenceStateDescriptor.enableTimeToLive(stateTtlConfig);
		// d. 实例化对象
		fenceState = getRuntimeContext().getMapState(fenceStateDescriptor) ;

		MapStateDescriptor<String, String> inTimeStateDescriptor = new MapStateDescriptor<String, String>("inTimeState", String.class, String.class);
		StateTtlConfig ttlConfig = StateTtlConfig
			.newBuilder(Time.milliseconds(parameterTool.getLong("state.ttl.millionseconds", 86400000L)))
			.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
			.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
			.build();
		inTimeStateDescriptor.enableTimeToLive(ttlConfig);
		inTimeState = getRuntimeContext().getMapState(inTimeStateDescriptor) ;
	}

	@Override
	public void apply(Tuple2<String, Integer> key, TimeWindow window,
	                  Iterable<ElectricFenceModel> input, Collector<ElectricFenceModel> out) throws Exception {
		// todo 1: 将窗口中数据转存到列表List中
		LinkedList<ElectricFenceModel> electricFenceModelList = Lists.newLinkedList(input);

		// todo 2: 对列表中数据按照终端时间降序排序
		Collections.sort(electricFenceModelList);  // 此时要求列表中数据时可排序的，实现接口Comparable

		// todo 3: 分别统计窗口中车辆数据在围栏内还围栏外个数
		long inTotal = electricFenceModelList.stream().filter(model -> model.getFenceStatus() == 1).count();
		long outTotal = electricFenceModelList.stream().filter(model -> model.getFenceStatus() == 0).count();

		// todo 4: 比较判断, 确定监控车辆在电子围栏内还是电子围栏外
		int currentStatus = (inTotal >= outTotal) ? 1 : 0 ; // 认为in 和out 个数相同时，属于in

		/*
							lastStatus          currentStatus
		 驶入围栏：				0（外）               1（内）
		                        0                    0     -> 不进行记录数据
		 --------------------------------------------------------------------------------
		 驶出围栏：               1（内）              0（外）
		                        1                   1      -> 不进行记录数据

		 */
		// todo 5: 获取以前状态 -> 电子围栏内还是外
		String vin = key.f0 ;
		Integer lastStatus = fenceState.get(vin) ;
		if(null == lastStatus){
			lastStatus = 0 ;// 电子围栏外
		}

		// todo 6: 驶入电子围栏内
		if(0 == lastStatus && 1 == currentStatus){
			// 获取窗口中第1条车辆数据：在电子围栏中
			ElectricFenceModel fenceModel = electricFenceModelList.stream()
				.filter(model -> model.getFenceStatus() == 1).findFirst().get();
			// 设置驶入电子围栏时间InTime
			fenceModel.setInTime(fenceModel.getGpsTime());

			// todo 8: 输出结果数据
			out.collect(fenceModel);

			// todo: 更新监控车辆驶入电子围栏时间
			inTimeState.put(vin, fenceModel.getInTime());
		}
		// todo 7: 驶出电子围栏
		else if(1 == lastStatus && 0 == currentStatus){
			// 获取窗口中第1条车辆数据：在电子围栏外
			ElectricFenceModel fenceModel = electricFenceModelList.stream()
				.filter(model -> model.getFenceStatus() == 0).findFirst().get();
			// 设置驶出时间
			fenceModel.setOutTime(fenceModel.getGpsTime());

			// 从状态中获取监控车辆驶入围栏时间
			String inTime = inTimeState.get(vin);
			fenceModel.setInTime(inTime);

			// todo 8: 输出结果
			out.collect(fenceModel);

			// todo: 清空状态中保存监控车辆输入电子围栏时间
			inTimeState.remove(vin);
		}

		// todo 9: 更新状态值为当前窗口中状态
		fenceState.put(vin, currentStatus);
	}

}
