package cn.cavehicle.streaming.function.window;

import cn.cavehicle.entity.OnlineDataModel;
import cn.cavehicle.entity.VehicleDataPartObj;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

/**
 * 实现窗口函数，针对远程诊断实时故障中窗口数据金蒜
 */
public class OnlineStatisticsWindowFunction
	extends RichWindowFunction<VehicleDataPartObj, OnlineDataModel, String, TimeWindow> {
	@Override
	public void apply(String vin, TimeWindow window,
	                  Iterable<VehicleDataPartObj> input, Collector<OnlineDataModel> out) throws Exception {
		// 创建窗口计算结果封装实体类对象
		OnlineDataModel onlineDataModel = new OnlineDataModel() ;

		// todo: 1. 将迭代器中车辆数据转存到列表中
		LinkedList<VehicleDataPartObj> vehicleDataPartObjList = Lists.newLinkedList(input);

		// todo: 2. 对窗口中数据进行升序排序，按照车辆终端时间
		vehicleDataPartObjList.sort(Comparator.comparingLong(VehicleDataPartObj::getTerminalTimeStamp));

		// todo: 3. 循环遍历窗口中车辆数据，过滤获取窗口中报警故障数据
		LinkedList<VehicleDataPartObj> alarmList = new LinkedList<>();
		for (VehicleDataPartObj vehicleDataPartObj : vehicleDataPartObjList) {
			// 判断每条车辆数据中，各个报警故障字段的值，如果有1个alarm字段值为1，说明此条数据为报警故障数据
			if(filterAlarm(vehicleDataPartObj)){
				alarmList.add(vehicleDataPartObj) ;
			}
		}

		// todo: 4. 判断是否有报警故障数据
		if(alarmList.size() > 0){
			// todo: 窗口中车辆数据存在报警故障数据
			// 4-1. 获取最后一条报警故障数据
			VehicleDataPartObj lastObj = alarmList.getLast();
			// 使用工具类拷贝属性值
			BeanUtils.copyProperties(onlineDataModel, lastObj);

			// 4-2. 设置报警相关属性
			onlineDataModel.setIsAlarm(1);
			onlineDataModel.setEarliestTime(alarmList.getFirst().getTerminalTime());

			// 4-3. 设报警故障名称
			List<String> alarmNameList = getAlarmNameList(lastObj);
			onlineDataModel.setAlarmName(String.join(",", alarmNameList));

			// 4-4. 设置充电标识
			Integer chargeState = getChargeState(lastObj.getChargeStatus());
			onlineDataModel.setChargeFlag(chargeState);
		}else{
			// todo：表示此窗口中，车辆数据没有报警故障数据
			onlineDataModel.setIsAlarm(0);
			// 使用工具类，拷贝属性值
			VehicleDataPartObj lastObj = vehicleDataPartObjList.getLast();
			BeanUtils.copyProperties(onlineDataModel, lastObj);
			// 设置充电标识
			Integer chargeState = getChargeState(lastObj.getChargeStatus());
			onlineDataModel.setChargeFlag(chargeState);
		}
		// 输出窗口中计算结果数据
		out.collect(onlineDataModel);
	}

	/**
	 * 依据充电状态返回充电标记
	 */
	private Integer getChargeState(Integer chargeStatus){
		/*
			 "0x01: 停车充电 0x02: 行车充电 0x03: 未充电  0x04:充电完成 0xFE: 异常 0xFF:无效"
		 */
		int chargeFlag ; // 充电状态变量
		// 充电状态："0x01: 停车充电 0x02: 行车充电
		if(chargeStatus == 1 || chargeStatus == 2){
			chargeFlag = 1;
		}else if(chargeStatus == 3 || chargeStatus == 4){
			chargeFlag = 0 ;
		}else{
			chargeFlag = 2 ;
		}
		return chargeFlag ;
	}

	/**
	 * 将车辆数据的故障名称追加到列表中
	 */
	private List<String> getAlarmNameList(VehicleDataPartObj dataPartObj){
		// 定义故障名称列表
		List<String> alarmNameList = new ArrayList<>() ;

		//电池高温报警
		if(dataPartObj.getBatteryAlarm() == 1) {
			alarmNameList.add("电池高温报警");
		}
		//单体电池高压报警
		if(dataPartObj.getSingleBatteryOverVoltageAlarm() == 1) {
			alarmNameList.add("单体电池高压报警");
		}
		//电池单体一致性差报警
		if(dataPartObj.getBatteryConsistencyDifferenceAlarm() == 1) {
			alarmNameList.add("电池单体一致性差报警");
		}
		//绝缘报警
		if(dataPartObj.getInsulationAlarm() == 1) {
			alarmNameList.add("绝缘报警");
		}
		//高压互锁状态报警
		if(dataPartObj.getHighVoltageInterlockStateAlarm() == 1) {
			alarmNameList.add("高压互锁状态报警");
		}
		//SOC跳变报警
		if(dataPartObj.getSocJumpAlarm() == 1) {
			alarmNameList.add("SOC跳变报警");
		}
		//驱动电机控制器温度报警
		if(dataPartObj.getDriveMotorControllerTemperatureAlarm() == 1) {
			alarmNameList.add("驱动电机控制器温度报警");
		}
		//DC-DC温度报警（dc-dc可以理解为车辆动力智能系统转换器）
		if(dataPartObj.getDcdcTemperatureAlarm() == 1) {
			alarmNameList.add("DC-DC温度报警");
		}
		//SOC过高报警
		if(dataPartObj.getSocHighAlarm() == 1) {
			alarmNameList.add("SOC过高报警");
		}
		//SOC低报警
		if(dataPartObj.getSocLowAlarm() == 1) {
			alarmNameList.add("SOC低报警");
		}
		//温度差异报警
		if(dataPartObj.getTemperatureDifferenceAlarm() == 1) {
			alarmNameList.add("温度差异报警");
		}
		//车载储能装置欠压报警
		if(dataPartObj.getVehicleStorageDeviceUndervoltageAlarm() == 1) {
			alarmNameList.add("车载储能装置欠压报警");
		}
		//DC-DC状态报警
		if(dataPartObj.getDcdcStatusAlarm() == 1) {
			alarmNameList.add("DC-DC状态报警");
		}
		//单体电池欠压报警
		if(dataPartObj.getSingleBatteryUnderVoltageAlarm() == 1) {
			alarmNameList.add("单体电池欠压报警");
		}
		//可充电储能系统不匹配报警
		if(dataPartObj.getRechargeableStorageDeviceMismatchAlarm() == 1) {
			alarmNameList.add("可充电储能系统不匹配报警");
		}
		//车载储能装置过压报警
		if(dataPartObj.getVehicleStorageDeviceOvervoltageAlarm() == 1) {
			alarmNameList.add("车载储能装置过压报警");
		}
		//制动系统报警
		if(dataPartObj.getBrakeSystemAlarm() == 1) {
			alarmNameList.add("制动系统报警");
		}
		//驱动电机温度报警
		if(dataPartObj.getDriveMotorTemperatureAlarm() == 1) {
			alarmNameList.add("驱动电机温度报警");
		}
		//车载储能装置类型过充报警
		if(dataPartObj.getVehiclePureDeviceTypeOvercharge() == 1) {
			alarmNameList.add("车载储能装置类型过充报警");
		}

		// 返回列表
		return alarmNameList ;
	}

	/**
	 * 判断对象中是否存在异常字段
	 */
	private boolean filterAlarm(VehicleDataPartObj dataPartObj){
		// 报警字段逐一判断值进行比较
		boolean isAlarm =
			//电池高温报警
			dataPartObj.getBatteryAlarm() == 1 ||
				//单体电池高压报警
				dataPartObj.getSingleBatteryOverVoltageAlarm() == 1 ||
				//电池单体一致性差报警
				dataPartObj.getBatteryConsistencyDifferenceAlarm() == 1 ||
				//绝缘报警
				dataPartObj.getInsulationAlarm() == 1 ||
				//高压互锁状态报警
				dataPartObj.getHighVoltageInterlockStateAlarm() == 1 ||
				//SOC跳变报警
				dataPartObj.getSocJumpAlarm() == 1 ||
				//驱动电机控制器温度报警
				dataPartObj.getDriveMotorControllerTemperatureAlarm() == 1 ||
				//DC-DC温度报警（dc-dc可以理解为车辆动力智能系统转换器）
				dataPartObj.getDcdcTemperatureAlarm() == 1 ||
				//SOC过高报警
				dataPartObj.getSocHighAlarm() == 1 ||
				//SOC低报警
				dataPartObj.getSocLowAlarm() == 1 ||
				//温度差异报警
				dataPartObj.getTemperatureDifferenceAlarm() == 1 ||
				//车载储能装置欠压报警
				dataPartObj.getVehicleStorageDeviceUndervoltageAlarm() == 1 ||
				//DC-DC状态报警
				dataPartObj.getDcdcStatusAlarm() == 1 ||
				//单体电池欠压报警
				dataPartObj.getSingleBatteryUnderVoltageAlarm() == 1 ||
				//可充电储能系统不匹配报警
				dataPartObj.getRechargeableStorageDeviceMismatchAlarm() == 1 ||
				//车载储能装置过压报警
				dataPartObj.getVehicleStorageDeviceOvervoltageAlarm() == 1 ||
				//制动系统报警
				dataPartObj.getBrakeSystemAlarm() == 1 ||
				//驱动电机温度报警
				dataPartObj.getDriveMotorTemperatureAlarm() == 1 ||
				//车载储能装置类型过充报警
				dataPartObj.getVehiclePureDeviceTypeOvercharge() == 1 ;

		// 返回判断结果值
		return isAlarm ;
	}
}
