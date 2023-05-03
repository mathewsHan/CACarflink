package cn.cavehicle.streaming.function.window;

import cn.cavehicle.entity.ElectricFenceModel;
import com.google.common.collect.Lists;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.LinkedList;

/**
 * 自定义WindowFunction窗口函数，实现窗口中数据处理：确定监控车辆（黑名单车辆）驶入还是驶出电子围栏。
 */
public class ElectricFenceWindowFunction
	extends RichWindowFunction<ElectricFenceModel, ElectricFenceModel, String, TimeWindow> {

	// a. 定义状态，存储vin在电子围栏内还是外
	private MapState<String, Integer> fenceState = null ;

	@Override
	public void open(Configuration parameters) throws Exception {
		// 全局参数
		ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

		// b. 创建状态描述符
		MapStateDescriptor<String, Integer> fenceStateDescriptor = new MapStateDescriptor<String, Integer>(
			"fenceState", String.class, Integer.class
		);
		// c. 设置状态TTL（Time To Live）
		StateTtlConfig ttlConfig = StateTtlConfig
			.newBuilder(Time.milliseconds(parameterTool.getLong("state.ttl.millionseconds", 86400000L)))
			.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
			.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
			.build();
		fenceStateDescriptor.enableTimeToLive(ttlConfig);
		// d. 实例化状态
		fenceState = getRuntimeContext().getMapState(fenceStateDescriptor) ;
	}

	@Override
	public void apply(String vin, TimeWindow window,
	                  Iterable<ElectricFenceModel> input, Collector<ElectricFenceModel> out) throws Exception {
		// todo 1: 将窗口中数据转存到列表list中
		LinkedList<ElectricFenceModel> electricFenceModelList = Lists.newLinkedList(input);

		// todo 2: 对窗口中数据按照终端时间terminalTime升序排序
		Collections.sort(electricFenceModelList);

		// todo 3: 分别获取窗口中数据在电子围栏内和电子围栏外个数
		long inTotal = electricFenceModelList.stream().filter(model -> model.getFenceStatus() == 1).count();
		long outTotal = electricFenceModelList.stream().filter(model -> model.getFenceStatus() == 0).count();

		// todo 4: 比较判断，确定车辆在电子围栏内还是电子围栏外（窗口中多条车辆数据）
		int currentStatus = (inTotal >= outTotal) ? 1 : 0 ;   // 认为 in 和 out 个数相同时，属于in，就是在电子围栏内

		// todo 5: 获取车辆以前状态，电子围栏内还是外
		Integer lastStatus = fenceState.get(vin) ;
		// 监控车辆第1个窗口之前默认状态没有记录，当做在电子围栏外
		if(null == lastStatus){
			lastStatus = 0 ;
		}
		System.out.println("vin: " + vin + ", 上次状态： " + lastStatus + ", 当前状态： " + currentStatus);

		/*
								lastStatus                  currentStatus
		驶入围栏： 外 -> 内		    0                           1
									1                           1                   --> 数据无需处理

		输出围栏： 内 -> 外            1                           0
									0                           0                   --> 数据无需处理
		 */
		// todo 6: 依据条件，判断驶入还是输出电子围栏，进行属性设置
		ElectricFenceModel fenceModel = new ElectricFenceModel() ;
		// 驶入电子围栏
		if(0 == lastStatus && 1 == currentStatus){
			// 获取窗口中第1条进入电子围栏数据
			ElectricFenceModel firstModel = electricFenceModelList
				.stream().filter(model -> model.getFenceStatus() == 1).findFirst().get();
			// 将第1条进入电子围数据中属性值进行拷贝，工具类：BeanUtils
			BeanUtils.copyProperties(fenceModel, firstModel);
			// 设置进入电子围栏时间
			fenceModel.setInTime(firstModel.getGpsTime());
			// todo 7: 输出数据
			out.collect(fenceModel);
		}
		// 驶出电子围栏
		else if(1 == lastStatus && 0 == currentStatus){
			// 获取窗口中第1条离开电子围栏数据
			ElectricFenceModel firstModel = electricFenceModelList
				.stream().filter(model -> model.getFenceStatus() == 0).findFirst().get();
			// 将第1条进入电子围数据中属性值进行拷贝，工具类：BeanUtils
			BeanUtils.copyProperties(fenceModel, firstModel);
			// 设置驶出电子围栏时间
			fenceModel.setOutTime(firstModel.getGpsTime());
			// todo 7: 输出数据
			out.collect(fenceModel);
		}

		// todo 8: 更新状态值为当前窗口中计算电子围栏状态值
		fenceState.put(vin, currentStatus);
	}

}
