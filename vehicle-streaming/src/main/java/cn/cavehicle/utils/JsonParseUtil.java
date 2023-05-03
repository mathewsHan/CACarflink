package cn.cavehicle.utils;

import cn.cavehicle.entity.VehicleDataObj;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

/**
 * 定义JSON解析类：将从Kafka消息队列获取车辆数据【JSON字符串】解析转换为VehicleDataObj对象
 */
public class JsonParseUtil {

	// 传递JSON字符串（单行字符串），解析封装为实体类对象
	public static VehicleDataObj parseJsonToBean(String jsonStr){
		// 定义实体类对象
		VehicleDataObj vehicleDataObj = new VehicleDataObj() ;

		/*
		实现步骤：
			1. 解析整体字符串{} -> JSONObject，封装到Map<String, Object>集合
			2. 获取单值key对应value，并设置到实体类对象
			3. 属性值为：字符串[] -> JSONArray，封装到List中，其中再次对列表中每条数据进行解析{} -> Map集合，使用JSONArray
				嵌套json解析，设置实体类对象
		 */
		try{
			// todo 1. 解析整体字符串{} -> JSONObject，封装到Map<String, Object>集合
			Map<String, Object> vehicleMap = jsonToMap(jsonStr);

			// todo 2. 获取单值key对应value，并设置到实体类对象
			setSingleValue(vehicleMap, vehicleDataObj) ;

			// todo 3. 属性值为：字符串[] -> JSONArray，封装到List中，其中再次对列表中每条数据进行解析{} -> Map集合，使用JSONArray
			setListValue(vehicleMap, vehicleDataObj);
		}catch (Exception e){
			System.out.println("JSON数据解析失败为异常数据：" + jsonStr);
			// todo: 当解析JSON字符异常，说明数据属于异常数据
			vehicleDataObj.setErrorData(jsonStr);
		}

		/*
			todo 4. 判断解析json字符串，是否为正常数据还是异常数据，如果是异常数据赋值字段：errorData
				vin 不为null, terminalTime 不为null, 经纬度不为null 时才是正常数据，否则就是异常数据
		 */
		if(vehicleDataObj.getVin().isEmpty() || vehicleDataObj.getTerminalTime().isEmpty() ||
			vehicleDataObj.getLng() == -999999D || vehicleDataObj.getLat() == -999999D){
			System.out.println("JSON数据解析后核心字段缺少值:" + jsonStr);
			// 设置扩展字段：errorData为json字符串值
			vehicleDataObj.setErrorData(jsonStr);
		}

		// todo 5. 转换数据事件时间字段String为时间抽Long类型，由于后续业务分析中基于事假时间窗口计算，方便直接使用
		if(!vehicleDataObj.getTerminalTime().isEmpty()){
			// 获取数据事件时间字段值
			String terminalTime = vehicleDataObj.getTerminalTime();
			// 转换字符串为Date日期
			Date date = DateUtil.convertStringToDate(terminalTime, DateUtil.DATE_TIME_FORMAT);
			// 获取时间戳
			long timestamp = date.getTime();
			// 设置对象属性值
			vehicleDataObj.setTerminalTimeStamp(timestamp);
		}

		// 返回解析对象
		return vehicleDataObj ;
	}

	/**
	 *  从Map集合中获取复杂字段的值，进一步解析封装，得到Map集合，依据属性获取值，设置到对象中
	 */
	private static void setListValue(Map<String, Object> vehicleMap, VehicleDataObj vehicleDataObj) {
		/* ------------------------------------------nevChargeSystemVoltageDtoList 可充电储能子系统电压信息列表-------------------------------------- */
		List<Map<String, Object>> nevChargeSystemVoltageDtoList = jsonToList(
			vehicleMap.getOrDefault("nevChargeSystemVoltageDtoList", "[]").toString()
		);
		if (!nevChargeSystemVoltageDtoList.isEmpty()) {
			// 只取list中的第一个map集合,集合中的第一条数据为有效数据
			Map<String, Object> nevChargeSystemVoltageDtoMap = nevChargeSystemVoltageDtoList.get(0);
			vehicleDataObj.setCurrentBatteryStartNum(convertIntType("currentBatteryStartNum",nevChargeSystemVoltageDtoMap));
			vehicleDataObj.setBatteryVoltage(convertStringType("batteryVoltage", nevChargeSystemVoltageDtoMap));
			vehicleDataObj.setChargeSystemVoltage(convertDoubleType("chargeSystemVoltage",nevChargeSystemVoltageDtoMap));
			vehicleDataObj.setCurrentBatteryCount(convertIntType("currentBatteryCount", nevChargeSystemVoltageDtoMap));
			vehicleDataObj.setBatteryCount(convertIntType("batteryCount", nevChargeSystemVoltageDtoMap));
			vehicleDataObj.setChildSystemNum(convertIntType("childSystemNum", nevChargeSystemVoltageDtoMap));
			vehicleDataObj.setChargeSystemCurrent(convertDoubleType("chargeSystemCurrent", nevChargeSystemVoltageDtoMap));
		}

		/* ----------------------------------------------driveMotorData-------------------------------------------------- */
		List<Map<String, Object>> driveMotorData = jsonToList(
			vehicleMap.getOrDefault("driveMotorData", "[]").toString()
		);                                    //驱动电机数据
		if (!driveMotorData.isEmpty()) {
			Map<String, Object> driveMotorMap = driveMotorData.get(0);
			vehicleDataObj.setControllerInputVoltage(convertDoubleType("controllerInputVoltage", driveMotorMap));
			vehicleDataObj.setControllerTemperature(convertDoubleType("controllerTemperature", driveMotorMap));
			vehicleDataObj.setRevolutionSpeed(convertDoubleType("revolutionSpeed", driveMotorMap));
			vehicleDataObj.setNum(convertIntType("num", driveMotorMap));
			vehicleDataObj.setControllerDcBusCurrent(convertDoubleType("controllerDcBusCurrent", driveMotorMap));
			vehicleDataObj.setTemperature(convertDoubleType("temperature", driveMotorMap));
			vehicleDataObj.setTorque(convertDoubleType("torque", driveMotorMap));
			vehicleDataObj.setState(convertIntType("state", driveMotorMap));
		}

		/* -----------------------------------------nevChargeSystemTemperatureDtoList------------------------------------ */
		List<Map<String, Object>> nevChargeSystemTemperatureDtoList = jsonToList(
			vehicleMap.getOrDefault("nevChargeSystemTemperatureDtoList", "[]").toString()
		);
		if (!nevChargeSystemTemperatureDtoList.isEmpty()) {
			Map<String, Object> nevChargeSystemTemperatureMap = nevChargeSystemTemperatureDtoList.get(0);
			vehicleDataObj.setProbeTemperatures(convertStringType("probeTemperatures", nevChargeSystemTemperatureMap));
			vehicleDataObj.setChargeTemperatureProbeNum(convertIntType("chargeTemperatureProbeNum", nevChargeSystemTemperatureMap));
		}

		/* --------------------------------------------ecuErrCodeDataList------------------------------------------------ */
		Map<String, Object> xcuerrinfoMap = jsonToMap(
			vehicleMap.getOrDefault("xcuerrinfo", "{}").toString()
		);
		if (!xcuerrinfoMap.isEmpty()) {
			List<Map<String, Object>> ecuErrCodeDataList = jsonToList(
				xcuerrinfoMap.getOrDefault("ecuErrCodeDataList", new ArrayList()).toString()
			) ;
			if (ecuErrCodeDataList.size() > 4) {
				vehicleDataObj.setVcuFaultCode(convertStringType("errCodes", ecuErrCodeDataList.get(0)));
				vehicleDataObj.setBcuFaultCodes(convertStringType("errCodes", ecuErrCodeDataList.get(1)));
				vehicleDataObj.setDcdcFaultCode(convertStringType("errCodes", ecuErrCodeDataList.get(2)));
				vehicleDataObj.setIpuFaultCodes(convertStringType("errCodes", ecuErrCodeDataList.get(3)));
				vehicleDataObj.setObcFaultCode(convertStringType("errCodes", ecuErrCodeDataList.get(4)));
			}
		}
	}

	/**
	 * 解析JSON字符串: [] -> JSONArray，最终封装Map集合中
	 */
	private static List<Map<String, Object>> jsonToList(String jsonStr) {
		// a. 创建JSONArray对象
		JSONArray jsonArray = new JSONArray(jsonStr);
		// b. 循环遍历，对列表汇总每个值，再次进行解析：{} -> map集合
		List<Map<String, Object>> list = new ArrayList<>();
		for(int index = 0; index < jsonArray.length(); index ++){
			String json = jsonArray.get(index).toString();
			// 再次解析
			Map<String, Object> map = jsonToMap(json);
			// 添加到列表
			list.add(map);
		}
		// c. 返回解析值
		return list ;
	}

	/**
	 * 从Map集合中依据属性名称获取值，如果没有使用默认值，设置到对象中
	 */
	private static void setSingleValue(Map<String, Object> vehicleMap, VehicleDataObj vehicleDataObj) {
		vehicleDataObj.setGearDriveForce(convertIntType("gearDriveForce", vehicleMap));
		vehicleDataObj.setBatteryConsistencyDifferenceAlarm(convertIntType("batteryConsistencyDifferenceAlarm", vehicleMap));
		vehicleDataObj.setSoc(convertIntType("soc", vehicleMap));
		vehicleDataObj.setSocJumpAlarm(convertIntType("socJumpAlarm", vehicleMap));
		vehicleDataObj.setCaterpillaringFunction(convertIntType("caterpillaringFunction", vehicleMap));
		vehicleDataObj.setSatNum(convertIntType("satNum", vehicleMap));
		vehicleDataObj.setSocLowAlarm(convertIntType("socLowAlarm", vehicleMap));
		vehicleDataObj.setChargingGunConnectionState(convertIntType("chargingGunConnectionState", vehicleMap));
		vehicleDataObj.setMinTemperatureSubSystemNum(convertIntType("minTemperatureSubSystemNum", vehicleMap));
		vehicleDataObj.setChargedElectronicLockStatus(convertIntType("chargedElectronicLockStatus", vehicleMap));
		vehicleDataObj.setMaxVoltageBatteryNum(convertIntType("maxVoltageBatteryNum", vehicleMap));
		vehicleDataObj.setTerminalTime(convertStringType("terminalTime", vehicleMap));
		vehicleDataObj.setSingleBatteryOverVoltageAlarm(convertIntType("singleBatteryOverVoltageAlarm", vehicleMap));
		vehicleDataObj.setOtherFaultCount(convertIntType("otherFaultCount", vehicleMap));
		vehicleDataObj.setVehicleStorageDeviceOvervoltageAlarm(convertIntType("vehicleStorageDeviceOvervoltageAlarm", vehicleMap));
		vehicleDataObj.setBrakeSystemAlarm(convertIntType("brakeSystemAlarm", vehicleMap));
		vehicleDataObj.setServerTime(convertStringType("serverTime", vehicleMap));
		vehicleDataObj.setVin(convertStringType("vin", vehicleMap).toUpperCase());
		vehicleDataObj.setRechargeableStorageDevicesFaultCount(convertIntType("rechargeableStorageDevicesFaultCount", vehicleMap));
		vehicleDataObj.setDriveMotorTemperatureAlarm(convertIntType("driveMotorTemperatureAlarm", vehicleMap));
		vehicleDataObj.setGearBrakeForce(convertIntType("gearBrakeForce", vehicleMap));
		vehicleDataObj.setDcdcStatusAlarm(convertIntType("dcdcStatusAlarm", vehicleMap));
		vehicleDataObj.setLat(convertDoubleType("lat", vehicleMap));
		vehicleDataObj.setDriveMotorFaultCodes(convertStringType("driveMotorFaultCodes", vehicleMap));
		vehicleDataObj.setDeviceType(convertStringType("deviceType", vehicleMap));
		vehicleDataObj.setVehicleSpeed(convertDoubleType("vehicleSpeed", vehicleMap));
		vehicleDataObj.setLng(convertDoubleType("lng", vehicleMap));
		vehicleDataObj.setChargingTimeExtensionReason(convertIntType("chargingTimeExtensionReason", vehicleMap));
		vehicleDataObj.setGpsTime(convertStringType("gpsTime", vehicleMap));
		vehicleDataObj.setEngineFaultCount(convertIntType("engineFaultCount", vehicleMap));
		vehicleDataObj.setCarId(convertStringType("carId", vehicleMap));
		vehicleDataObj.setCurrentElectricity(convertDoubleType("vehicleSpeed", vehicleMap));
		vehicleDataObj.setSingleBatteryUnderVoltageAlarm(convertIntType("singleBatteryUnderVoltageAlarm", vehicleMap));
		vehicleDataObj.setMaxVoltageBatterySubSystemNum(convertIntType("maxVoltageBatterySubSystemNum", vehicleMap));
		vehicleDataObj.setMinTemperatureProbe(convertIntType("minTemperatureProbe", vehicleMap));
		vehicleDataObj.setDriveMotorNum(convertIntType("driveMotorNum", vehicleMap));
		vehicleDataObj.setTotalVoltage(convertDoubleType("totalVoltage", vehicleMap));
		vehicleDataObj.setTemperatureDifferenceAlarm(convertIntType("temperatureDifferenceAlarm", vehicleMap));
		vehicleDataObj.setMaxAlarmLevel(convertIntType("maxAlarmLevel", vehicleMap));
		vehicleDataObj.setStatus(convertIntType("status", vehicleMap));
		vehicleDataObj.setGeerPosition(convertIntType("geerPosition", vehicleMap));
		vehicleDataObj.setAverageEnergyConsumption(convertDoubleType("averageEnergyConsumption", vehicleMap));
		vehicleDataObj.setMinVoltageBattery(convertDoubleType("minVoltageBattery", vehicleMap));
		vehicleDataObj.setGeerStatus(convertIntType("geerStatus", vehicleMap));
		vehicleDataObj.setMinVoltageBatteryNum(convertIntType("minVoltageBatteryNum", vehicleMap));
		vehicleDataObj.setValidGps(convertStringType("validGps", vehicleMap));
		vehicleDataObj.setEngineFaultCodes(convertStringType("engineFaultCodes", vehicleMap));
		vehicleDataObj.setMinTemperatureValue(convertDoubleType("minTemperatureValue", vehicleMap));
		vehicleDataObj.setChargeStatus(convertIntType("chargeStatus", vehicleMap));
		vehicleDataObj.setIgnitionTime(convertStringType("ignitionTime", vehicleMap));
		vehicleDataObj.setTotalOdometer(convertDoubleType("totalOdometer", vehicleMap));
		vehicleDataObj.setAlti(convertDoubleType("alti", vehicleMap));
		vehicleDataObj.setSpeed(convertDoubleType("speed", vehicleMap));
		vehicleDataObj.setSocHighAlarm(convertIntType("socHighAlarm", vehicleMap));
		vehicleDataObj.setVehicleStorageDeviceUndervoltageAlarm(convertIntType("vehicleStorageDeviceUndervoltageAlarm", vehicleMap));
		vehicleDataObj.setTotalCurrent(convertDoubleType("totalCurrent", vehicleMap));
		vehicleDataObj.setBatteryAlarm(convertIntType("batteryAlarm", vehicleMap));
		vehicleDataObj.setRechargeableStorageDeviceMismatchAlarm(convertIntType("rechargeableStorageDeviceMismatchAlarm", vehicleMap));
		vehicleDataObj.setIsHistoryPoi(convertIntType("isHistoryPoi", vehicleMap));
		vehicleDataObj.setVehiclePureDeviceTypeOvercharge(convertIntType("vehiclePureDeviceTypeOvercharge", vehicleMap));
		vehicleDataObj.setMaxVoltageBattery(convertDoubleType("maxVoltageBattery", vehicleMap));
		vehicleDataObj.setDcdcTemperatureAlarm(convertIntType("dcdcTemperatureAlarm", vehicleMap));
		vehicleDataObj.setIsValidGps(convertStringType("isValidGps", vehicleMap));
		vehicleDataObj.setLastUpdatedTime(convertStringType("lastUpdatedTime", vehicleMap));
		vehicleDataObj.setDriveMotorControllerTemperatureAlarm(convertIntType("driveMotorControllerTemperatureAlarm", vehicleMap));
		vehicleDataObj.setIgniteCumulativeMileage(convertDoubleType("igniteCumulativeMileage", vehicleMap));
		vehicleDataObj.setDcStatus(convertIntType("dcStatus", vehicleMap));
		vehicleDataObj.setRepay(convertStringType("repay", vehicleMap));
		vehicleDataObj.setMaxTemperatureSubSystemNum(convertIntType("maxTemperatureSubSystemNum", vehicleMap));
		vehicleDataObj.setMinVoltageBatterySubSystemNum(convertIntType("minVoltageBatterySubSystemNum", vehicleMap));
		vehicleDataObj.setHeading(convertDoubleType("heading", vehicleMap));
		vehicleDataObj.setTuid(convertStringType("tuid", vehicleMap));
		vehicleDataObj.setEnergyRecoveryStatus(convertIntType("energyRecoveryStatus", vehicleMap));
		vehicleDataObj.setFireStatus(convertIntType("fireStatus", vehicleMap));
		vehicleDataObj.setTargetType(convertStringType("targetType", vehicleMap));
		vehicleDataObj.setMaxTemperatureProbe(convertIntType("maxTemperatureProbe", vehicleMap));
		vehicleDataObj.setRechargeableStorageDevicesFaultCodes(convertStringType("rechargeableStorageDevicesFaultCodes", vehicleMap));
		vehicleDataObj.setCarMode(convertIntType("carMode", vehicleMap));
		vehicleDataObj.setHighVoltageInterlockStateAlarm(convertIntType("highVoltageInterlockStateAlarm", vehicleMap));
		vehicleDataObj.setInsulationAlarm(convertIntType("insulationAlarm", vehicleMap));
		vehicleDataObj.setMileageInformation(convertIntType("mileageInformation", vehicleMap));
		vehicleDataObj.setMaxTemperatureValue(convertDoubleType("maxTemperatureValue", vehicleMap));
		vehicleDataObj.setOtherFaultCodes(convertStringType("otherFaultCodes", vehicleMap));
		vehicleDataObj.setRemainPower(convertDoubleType("remainPower", vehicleMap));
		vehicleDataObj.setInsulateResistance(convertIntType("insulateResistance", vehicleMap));
		vehicleDataObj.setBatteryLowTemperatureHeater(convertIntType("batteryLowTemperatureHeater", vehicleMap));
		vehicleDataObj.setFuelConsumption100km(convertStringType("fuelConsumption100km", vehicleMap));
		vehicleDataObj.setFuelConsumption(convertStringType("fuelConsumption", vehicleMap));
		vehicleDataObj.setEngineSpeed(convertStringType("engineSpeed", vehicleMap));
		vehicleDataObj.setEngineStatus(convertStringType("engineStatus", vehicleMap));
		vehicleDataObj.setTrunk(convertIntType("trunk", vehicleMap));
		vehicleDataObj.setLowBeam(convertIntType("lowBeam", vehicleMap));
		vehicleDataObj.setTriggerLatchOverheatProtect(convertStringType("triggerLatchOverheatProtect", vehicleMap));
		vehicleDataObj.setTurnLndicatorRight(convertIntType("turnLndicatorRight", vehicleMap));
		vehicleDataObj.setHighBeam(convertIntType("highBeam", vehicleMap));
		vehicleDataObj.setTurnLndicatorLeft(convertIntType("turnLndicatorLeft", vehicleMap));
		vehicleDataObj.setBcuSwVers(convertIntType("bcuSwVers", vehicleMap));
		vehicleDataObj.setBcuHwVers(convertIntType("bcuHwVers", vehicleMap));
		vehicleDataObj.setBcuOperMod(convertIntType("bcuOperMod", vehicleMap));
		vehicleDataObj.setChrgEndReason(convertIntType("chrgEndReason", vehicleMap));
		vehicleDataObj.setBCURegenEngDisp(convertStringType("BCURegenEngDisp", vehicleMap));
		vehicleDataObj.setBCURegenCpDisp(convertIntType("BCURegenCpDisp", vehicleMap));
		vehicleDataObj.setBcuChrgMod(convertIntType("bcuChrgMod", vehicleMap));
		vehicleDataObj.setBatteryChargeStatus(convertIntType("batteryChargeStatus", vehicleMap));
		vehicleDataObj.setBcuFltRnk(convertIntType("bcuFltRnk", vehicleMap));
		vehicleDataObj.setBattPoleTOver(convertStringType("battPoleTOver", vehicleMap));
		vehicleDataObj.setBcuSOH(convertDoubleType("bcuSOH", vehicleMap));
		vehicleDataObj.setBattIntrHeatActive(convertIntType("battIntrHeatActive", vehicleMap));
		vehicleDataObj.setBattIntrHeatReq(convertIntType("battIntrHeatReq", vehicleMap));
		vehicleDataObj.setBCUBattTarT(convertStringType("BCUBattTarT", vehicleMap));
		vehicleDataObj.setBattExtHeatReq(convertIntType("battExtHeatReq", vehicleMap));
		vehicleDataObj.setBCUMaxChrgPwrLongT(convertStringType("BCUMaxChrgPwrLongT", vehicleMap));
		vehicleDataObj.setBCUMaxDchaPwrLongT(convertStringType("BCUMaxDchaPwrLongT", vehicleMap));
		vehicleDataObj.setBCUTotalRegenEngDisp(convertStringType("BCUTotalRegenEngDisp", vehicleMap));
		vehicleDataObj.setBCUTotalRegenCpDisp(convertStringType("BCUTotalRegenCpDisp ", vehicleMap));
		vehicleDataObj.setDcdcFltRnk(convertIntType("dcdcFltRnk", vehicleMap));
		vehicleDataObj.setDcdcOutpCrrt(convertDoubleType("dcdcOutpCrrt", vehicleMap));
		vehicleDataObj.setDcdcOutpU(convertDoubleType("dcdcOutpU", vehicleMap));
		vehicleDataObj.setDcdcAvlOutpPwr(convertIntType("dcdcAvlOutpPwr", vehicleMap));
		vehicleDataObj.setAbsActiveStatus(convertStringType("absActiveStatus", vehicleMap));
		vehicleDataObj.setAbsStatus(convertStringType("absStatus", vehicleMap));
		vehicleDataObj.setVcuBrkErr(convertStringType("VcuBrkErr", vehicleMap));
		vehicleDataObj.setEPB_AchievedClampForce(convertStringType("EPB_AchievedClampForce", vehicleMap));
		vehicleDataObj.setEpbSwitchPosition(convertStringType("epbSwitchPosition", vehicleMap));
		vehicleDataObj.setEpbStatus(convertStringType("epbStatus", vehicleMap));
		vehicleDataObj.setEspActiveStatus(convertStringType("espActiveStatus", vehicleMap));
		vehicleDataObj.setEspFunctionStatus(convertStringType("espFunctionStatus", vehicleMap));
		vehicleDataObj.setESP_TCSFailStatus(convertStringType("ESP_TCSFailStatus", vehicleMap));
		vehicleDataObj.setHhcActive(convertStringType("hhcActive", vehicleMap));
		vehicleDataObj.setTcsActive(convertStringType("tcsActive", vehicleMap));
		vehicleDataObj.setEspMasterCylinderBrakePressure(convertStringType("espMasterCylinderBrakePressure", vehicleMap));
		vehicleDataObj.setESP_MasterCylinderBrakePressureValid(convertStringType("ESP_MasterCylinderBrakePressureValid", vehicleMap));
		vehicleDataObj.setEspTorqSensorStatus(convertStringType("espTorqSensorStatus", vehicleMap));
		vehicleDataObj.setEPS_EPSFailed(convertStringType("EPS_EPSFailed", vehicleMap));
		vehicleDataObj.setSasFailure(convertStringType("sasFailure", vehicleMap));
		vehicleDataObj.setSasSteeringAngleSpeed(convertStringType("sasSteeringAngleSpeed", vehicleMap));
		vehicleDataObj.setSasSteeringAngle(convertStringType("sasSteeringAngle", vehicleMap));
		vehicleDataObj.setSasSteeringAngleValid(convertStringType("sasSteeringAngleValid", vehicleMap));
		vehicleDataObj.setEspSteeringTorque(convertStringType("espSteeringTorque", vehicleMap));
		vehicleDataObj.setAcReq(convertIntType("acReq", vehicleMap));
		vehicleDataObj.setAcSystemFailure(convertIntType("acSystemFailure", vehicleMap));
		vehicleDataObj.setPtcPwrAct(convertDoubleType("ptcPwrAct", vehicleMap));
		vehicleDataObj.setPlasmaStatus(convertIntType("plasmaStatus", vehicleMap));
		vehicleDataObj.setBattInTemperature(convertIntType("battInTemperature", vehicleMap));
		vehicleDataObj.setBattWarmLoopSts(convertStringType("battWarmLoopSts", vehicleMap));
		vehicleDataObj.setBattCoolngLoopSts(convertStringType("battCoolngLoopSts", vehicleMap));
		vehicleDataObj.setBattCoolActv(convertStringType("battCoolActv", vehicleMap));
		vehicleDataObj.setMotorOutTemperature(convertIntType("motorOutTemperature", vehicleMap));
		vehicleDataObj.setPowerStatusFeedBack(convertStringType("powerStatusFeedBack", vehicleMap));
		vehicleDataObj.setAC_RearDefrosterSwitch(convertIntType("AC_RearDefrosterSwitch", vehicleMap));
		vehicleDataObj.setRearFoglamp(convertIntType("rearFoglamp", vehicleMap));
		vehicleDataObj.setDriverDoorLock(convertIntType("driverDoorLock", vehicleMap));
		vehicleDataObj.setAcDriverReqTemp(convertDoubleType("acDriverReqTemp", vehicleMap));
		vehicleDataObj.setKeyAlarm(convertIntType("keyAlarm", vehicleMap));
		vehicleDataObj.setAirCleanStsRemind(convertIntType("airCleanStsRemind", vehicleMap));
		vehicleDataObj.setRecycleType(convertIntType("recycleType", vehicleMap));
		vehicleDataObj.setStartControlsignal(convertStringType("startControlsignal", vehicleMap));
		vehicleDataObj.setAirBagWarningLamp(convertIntType("airBagWarningLamp", vehicleMap));
		vehicleDataObj.setFrontDefrosterSwitch(convertIntType("frontDefrosterSwitch", vehicleMap));
		vehicleDataObj.setFrontBlowType(convertStringType("frontBlowType", vehicleMap));
		vehicleDataObj.setFrontReqWindLevel(convertIntType("frontReqWindLevel", vehicleMap));
		vehicleDataObj.setBcmFrontWiperStatus(convertStringType("bcmFrontWiperStatus", vehicleMap));
		vehicleDataObj.setTmsPwrAct(convertStringType("tmsPwrAct", vehicleMap));
		vehicleDataObj.setKeyUndetectedAlarmSign(convertIntType("keyUndetectedAlarmSign", vehicleMap));
		vehicleDataObj.setPositionLamp(convertStringType("positionLamp", vehicleMap));
		vehicleDataObj.setDriverReqTempModel(convertIntType("driverReqTempModel", vehicleMap));
		vehicleDataObj.setTurnLightSwitchSts(convertIntType("turnLightSwitchSts", vehicleMap));
		vehicleDataObj.setAutoHeadlightStatus(convertIntType("autoHeadlightStatus", vehicleMap));
		vehicleDataObj.setDriverDoor(convertIntType("driverDoor", vehicleMap));
		vehicleDataObj.setFrntIpuFltRnk(convertIntType("frntIpuFltRnk", vehicleMap));
		vehicleDataObj.setFrontIpuSwVers(convertStringType("frontIpuSwVers", vehicleMap));
		vehicleDataObj.setFrontIpuHwVers(convertIntType("frontIpuHwVers", vehicleMap));
		vehicleDataObj.setFrntMotTqLongTermMax(convertIntType("frntMotTqLongTermMax", vehicleMap));
		vehicleDataObj.setFrntMotTqLongTermMin(convertIntType("frntMotTqLongTermMin", vehicleMap));
		vehicleDataObj.setCpvValue(convertIntType("cpvValue", vehicleMap));
		vehicleDataObj.setObcChrgSts(convertIntType("obcChrgSts", vehicleMap));
		vehicleDataObj.setObcFltRnk(convertStringType("obcFltRnk", vehicleMap));
		vehicleDataObj.setObcChrgInpAcI(convertDoubleType("obcChrgInpAcI", vehicleMap));
		vehicleDataObj.setObcChrgInpAcU(convertIntType("obcChrgInpAcU", vehicleMap));
		vehicleDataObj.setObcChrgDcI(convertDoubleType("obcChrgDcI", vehicleMap));
		vehicleDataObj.setObcChrgDcU(convertDoubleType("obcChrgDcU", vehicleMap));
		vehicleDataObj.setObcTemperature(convertIntType("obcTemperature", vehicleMap));
		vehicleDataObj.setObcMaxChrgOutpPwrAvl(convertIntType("obcMaxChrgOutpPwrAvl", vehicleMap));
		vehicleDataObj.setPassengerBuckleSwitch(convertIntType("passengerBuckleSwitch", vehicleMap));
		vehicleDataObj.setCrashlfo(convertStringType("crashlfo", vehicleMap));
		vehicleDataObj.setDriverBuckleSwitch(convertIntType("driverBuckleSwitch", vehicleMap));
		vehicleDataObj.setEngineStartHibit(convertStringType("engineStartHibit", vehicleMap));
		vehicleDataObj.setLockCommand(convertStringType("lockCommand", vehicleMap));
		vehicleDataObj.setSearchCarReq(convertStringType("searchCarReq", vehicleMap));
		vehicleDataObj.setAcTempValueReq(convertStringType("acTempValueReq", vehicleMap));
		vehicleDataObj.setVcuErrAmnt(convertStringType("vcuErrAmnt", vehicleMap));
		vehicleDataObj.setVcuSwVers(convertIntType("vcuSwVers", vehicleMap));
		vehicleDataObj.setVcuHwVers(convertIntType("vcuHwVers", vehicleMap));
		vehicleDataObj.setLowSpdWarnStatus(convertStringType("lowSpdWarnStatus", vehicleMap));
		vehicleDataObj.setLowBattChrgRqe(convertIntType("lowBattChrgRqe", vehicleMap));
		vehicleDataObj.setLowBattChrgSts(convertStringType("lowBattChrgSts", vehicleMap));
		vehicleDataObj.setLowBattU(convertDoubleType("lowBattU", vehicleMap));
		vehicleDataObj.setHandlebrakeStatus(convertIntType("handlebrakeStatus", vehicleMap));
		vehicleDataObj.setShiftPositionValid(convertStringType("shiftPositionValid", vehicleMap));
		vehicleDataObj.setAccPedalValid(convertStringType("accPedalValid", vehicleMap));
		vehicleDataObj.setDriveMode(convertIntType("driveMode", vehicleMap));
		vehicleDataObj.setDriveModeButtonStatus(convertIntType("driveModeButtonStatus", vehicleMap));
		vehicleDataObj.setVCUSRSCrashOutpSts(convertIntType("VCUSRSCrashOutpSts", vehicleMap));
		vehicleDataObj.setTextDispEna(convertIntType("textDispEna", vehicleMap));
		vehicleDataObj.setCrsCtrlStatus(convertIntType("crsCtrlStatus", vehicleMap));
		vehicleDataObj.setCrsTarSpd(convertIntType("crsTarSpd", vehicleMap));
		vehicleDataObj.setCrsTextDisp(convertIntType("crsTextDisp",vehicleMap ));
		vehicleDataObj.setKeyOn(convertIntType("keyOn", vehicleMap));
		vehicleDataObj.setVehPwrlim(convertIntType("vehPwrlim", vehicleMap));
		vehicleDataObj.setVehCfgInfo(convertStringType("vehCfgInfo", vehicleMap));
		vehicleDataObj.setVacBrkPRmu(convertIntType("vacBrkPRmu", vehicleMap));
	}

	/**
	 * 解析json字符串：{} -> 使用JSONObject解析，属性和值到Map集合中
	 */
	public static Map<String, Object> jsonToMap(String jsonStr){
		// a. 创建JSONObject对象
		JSONObject jsonObject = new JSONObject(jsonStr);
		// b. 获取所有key：json字符串中属性名称
		Set<String> keySet = jsonObject.keySet();
		// c. 遍历属性名称，获取每个对应值，设置到Map集合中
		Map<String, Object> map = new HashMap<>();
		for (String key : keySet) {
			map.put(key, jsonObject.get(key)) ;
		}
		// d. 返回
		return map ;
	}

	/**
	 * 从Map集合中获取key值，转换为String
	 */
	private static String convertStringType(String fieldName, Map<String, Object> map){
		return map.getOrDefault(fieldName, "").toString();
	}

	/**
	 * 从Map集合中获取key值，转换为int
	 */
	private static int convertIntType(String fieldName, Map<String, Object> map){
		String value = map.getOrDefault(fieldName, -999999).toString();
		// 转换为int类型
		return Integer.parseInt(value);
	}

	/**
	 * 从Map集合中获取key值，转换为double
	 */
	private static double convertDoubleType(String fieldName, Map<String, Object> map){
		String value = map.getOrDefault(fieldName, -999999D).toString();
		// 转换为int类型
		return Double.parseDouble(value);
	}

}
