package cn.cavehicle.streaming.sink;

import cn.cavehicle.utils.DateUtil;
import cn.cavehicle.entity.VehicleDataObj;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 自定义Sink实现：将正确数据实时写入HBase数据库表中，后期集成Phoenix进行即席查询
 */
public class SrcDataToHBaseSink extends RichSinkFunction<VehicleDataObj> {

	// 定义变量，表名称，外部传递
	private String tableName ;

	// 使用构造方法，接收表的名称
	public SrcDataToHBaseSink(String tableName){
		this.tableName = tableName ;
	}

	// 定义一个常量
	private static final byte[] cfBytes= Bytes.toBytes("INFO") ;

	// 定义变量
	private Connection connection = null ;
	private Table table = null ;

	// 准备工作，比如创建连接
	@Override
	public void open(Configuration parameters) throws Exception {
		// todo: 获取job全局参数
		ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
		String zkQuorum = parameterTool.getRequired("zookeeper.quorum") ;
		String zkPort = parameterTool.getRequired("zookeeper.clientPort") ;

		// a. 创建配置对象，设置ZK集群地址
		org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
		configuration.set(HConstants.CLIENT_ZOOKEEPER_QUORUM, zkQuorum);
		configuration.set(HConstants.CLIENT_ZOOKEEPER_CLIENT_PORT, zkPort);

		// b. 初始化Connection对象
		connection = ConnectionFactory.createConnection(configuration) ;

		// c. 实例化Table对象
		table = connection.getTable(TableName.valueOf(tableName)) ;
	}

	// 流中每条数据处理
	@Override
	public void invoke(VehicleDataObj vehicleDataObj, Context context) throws Exception {
		/*
			RowKey: reverse(vin) + _ + terminalTime
		 */
		// d. 创建Put对象
		Put put = createPut(vehicleDataObj) ;
		// e. 将put对象数据插入到表中
		table.put(put);
	}

	/**
	 * 将数据流中每条数据，封装到Put对象中，todo：向HBase表中写入数据时，所有值都是byte[] 字节数组，使用工具类Bytes
	 */
	private Put createPut(VehicleDataObj vehicleDataObj) {
		// step1、RowKey创建
		String rowKey = StringUtils.reverse(vehicleDataObj.getVin()) + "_" + vehicleDataObj.getTerminalTime();

		// step2、构建Put对象
		Put put = new Put(Bytes.toBytes(rowKey)) ;

		// step3、添加列
		put.addColumn(cfBytes, Bytes.toBytes("vin"), Bytes.toBytes(vehicleDataObj.getVin()));
		put.addColumn(cfBytes, Bytes.toBytes("terminalTime"), Bytes.toBytes(vehicleDataObj.getTerminalTime())) ;
		if (vehicleDataObj.getSoc() != -999999) put.addColumn(cfBytes, Bytes.toBytes("soc"), Bytes.toBytes(String.valueOf(vehicleDataObj.getSoc())));
		if (vehicleDataObj.getLat() != -999999) put.addColumn(cfBytes, Bytes.toBytes("lat"), Bytes.toBytes(String.valueOf(vehicleDataObj.getLat())));
		if (vehicleDataObj.getLng() != -999999) put.addColumn(cfBytes, Bytes.toBytes("lng"), Bytes.toBytes(String.valueOf(vehicleDataObj.getLng())));
		if (vehicleDataObj.getGearDriveForce() != -999999) put.addColumn(cfBytes, Bytes.toBytes("gearDriveForce"), Bytes.toBytes(String.valueOf(vehicleDataObj.getGearDriveForce())));
		if (vehicleDataObj.getBatteryConsistencyDifferenceAlarm() != -999999) put.addColumn(cfBytes, Bytes.toBytes("batteryConsistencyDifferenceAlarm"), Bytes.toBytes(String.valueOf(vehicleDataObj.getBatteryConsistencyDifferenceAlarm())));
		if (vehicleDataObj.getSocJumpAlarm() != -999999) put.addColumn(cfBytes, Bytes.toBytes("socJumpAlarm"), Bytes.toBytes(String.valueOf(vehicleDataObj.getSocJumpAlarm())));
		if (vehicleDataObj.getCaterpillaringFunction() != -999999) put.addColumn(cfBytes, Bytes.toBytes("caterpillaringFunction"), Bytes.toBytes(String.valueOf(vehicleDataObj.getCaterpillaringFunction())));
		if (vehicleDataObj.getSatNum() != -999999) put.addColumn(cfBytes, Bytes.toBytes("satNum"), Bytes.toBytes(String.valueOf(vehicleDataObj.getSatNum())));
		if (vehicleDataObj.getSocLowAlarm() != -999999) put.addColumn(cfBytes, Bytes.toBytes("socLowAlarm"), Bytes.toBytes(String.valueOf(vehicleDataObj.getSocLowAlarm())));
		if (vehicleDataObj.getChargingGunConnectionState() != -999999) put.addColumn(cfBytes, Bytes.toBytes("chargingGunConnectionState"), Bytes.toBytes(String.valueOf(vehicleDataObj.getChargingGunConnectionState())));
		if (vehicleDataObj.getMinTemperatureSubSystemNum() != -999999) put.addColumn(cfBytes, Bytes.toBytes("minTemperatureSubSystemNum"), Bytes.toBytes(String.valueOf(vehicleDataObj.getMinTemperatureSubSystemNum())));
		if (vehicleDataObj.getChargedElectronicLockStatus() != -999999) put.addColumn(cfBytes, Bytes.toBytes("chargedElectronicLockStatus"), Bytes.toBytes(String.valueOf(vehicleDataObj.getChargedElectronicLockStatus())));
		if (vehicleDataObj.getMaxVoltageBatteryNum() != -999999) put.addColumn(cfBytes, Bytes.toBytes("maxVoltageBatteryNum"), Bytes.toBytes(String.valueOf(vehicleDataObj.getMaxVoltageBatteryNum())));
		if (vehicleDataObj.getSingleBatteryOverVoltageAlarm() != -999999) put.addColumn(cfBytes, Bytes.toBytes("singleBatteryOverVoltageAlarm"), Bytes.toBytes(String.valueOf(vehicleDataObj.getSingleBatteryOverVoltageAlarm())));
		if (vehicleDataObj.getOtherFaultCount() != -999999) put.addColumn(cfBytes, Bytes.toBytes("otherFaultCount"), Bytes.toBytes(String.valueOf(vehicleDataObj.getOtherFaultCount())));
		if (vehicleDataObj.getVehicleStorageDeviceOvervoltageAlarm() != -999999) put.addColumn(cfBytes, Bytes.toBytes("vehicleStorageDeviceOvervoltageAlarm"), Bytes.toBytes(String.valueOf(vehicleDataObj.getVehicleStorageDeviceOvervoltageAlarm())));
		if (vehicleDataObj.getBrakeSystemAlarm() != -999999) put.addColumn(cfBytes, Bytes.toBytes("brakeSystemAlarm"), Bytes.toBytes(String.valueOf(vehicleDataObj.getBrakeSystemAlarm())));
		if (!vehicleDataObj.getServerTime().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("serverTime"), Bytes.toBytes(vehicleDataObj.getServerTime()));
		if (vehicleDataObj.getRechargeableStorageDevicesFaultCount() != -999999) put.addColumn(cfBytes, Bytes.toBytes("rechargeableStorageDevicesFaultCount"), Bytes.toBytes(String.valueOf(vehicleDataObj.getRechargeableStorageDevicesFaultCount())));
		if (vehicleDataObj.getDriveMotorTemperatureAlarm() != -999999) put.addColumn(cfBytes, Bytes.toBytes("driveMotorTemperatureAlarm"), Bytes.toBytes(String.valueOf(vehicleDataObj.getDriveMotorTemperatureAlarm())));
		if (vehicleDataObj.getGearBrakeForce() != -999999) put.addColumn(cfBytes, Bytes.toBytes("gearBrakeForce"), Bytes.toBytes(String.valueOf(vehicleDataObj.getGearBrakeForce())));
		if (vehicleDataObj.getDcdcStatusAlarm() != -999999) put.addColumn(cfBytes, Bytes.toBytes("dcdcStatusAlarm"), Bytes.toBytes(String.valueOf(vehicleDataObj.getDcdcStatusAlarm())));
		if (!vehicleDataObj.getDriveMotorFaultCodes().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("driveMotorFaultCodes"), Bytes.toBytes(vehicleDataObj.getDriveMotorFaultCodes()));
		if (!vehicleDataObj.getDeviceType().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("deviceType"), Bytes.toBytes(vehicleDataObj.getDeviceType()));
		if (vehicleDataObj.getVehicleSpeed() != -999999) put.addColumn(cfBytes, Bytes.toBytes("vehicleSpeed"), Bytes.toBytes(String.valueOf(vehicleDataObj.getVehicleSpeed())));
		if (vehicleDataObj.getChargingTimeExtensionReason() != -999999) put.addColumn(cfBytes, Bytes.toBytes("chargingTimeExtensionReason"), Bytes.toBytes(String.valueOf(vehicleDataObj.getChargingTimeExtensionReason())));
		if (vehicleDataObj.getCurrentBatteryStartNum() != -999999) put.addColumn(cfBytes, Bytes.toBytes("currentBatteryStartNum"), Bytes.toBytes(String.valueOf(vehicleDataObj.getCurrentBatteryStartNum())));
		if (!vehicleDataObj.getBatteryVoltage().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("batteryVoltage"), Bytes.toBytes(vehicleDataObj.getBatteryVoltage()));
		if (vehicleDataObj.getChargeSystemVoltage() != -999999) put.addColumn(cfBytes, Bytes.toBytes("chargeSystemVoltage"), Bytes.toBytes(String.valueOf(vehicleDataObj.getChargeSystemVoltage())));
		if (vehicleDataObj.getCurrentBatteryCount() != -999999) put.addColumn(cfBytes, Bytes.toBytes("currentBatteryCount"), Bytes.toBytes(String.valueOf(vehicleDataObj.getCurrentBatteryCount())));
		if (vehicleDataObj.getBatteryCount() != -999999) put.addColumn(cfBytes, Bytes.toBytes("batteryCount"), Bytes.toBytes(String.valueOf(vehicleDataObj.getBatteryCount())));
		if (vehicleDataObj.getChildSystemNum() != -999999) put.addColumn(cfBytes, Bytes.toBytes("childSystemNum"), Bytes.toBytes(String.valueOf(vehicleDataObj.getChildSystemNum())));
		if (vehicleDataObj.getChargeSystemCurrent() != -999999) put.addColumn(cfBytes, Bytes.toBytes("chargeSystemCurrent"), Bytes.toBytes(String.valueOf(vehicleDataObj.getChargeSystemCurrent())));
		if (!vehicleDataObj.getGpsTime().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("gpsTime"), Bytes.toBytes(vehicleDataObj.getGpsTime()));
		if (vehicleDataObj.getEngineFaultCount() != -999999) put.addColumn(cfBytes, Bytes.toBytes("engineFaultCount"), Bytes.toBytes(String.valueOf(vehicleDataObj.getEngineFaultCount())));
		if (!vehicleDataObj.getCarId().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("carId"), Bytes.toBytes(vehicleDataObj.getCarId()));
		if (vehicleDataObj.getCurrentElectricity() != -999999) put.addColumn(cfBytes, Bytes.toBytes("currentElectricity"), Bytes.toBytes(String.valueOf(vehicleDataObj.getCurrentElectricity())));
		if (vehicleDataObj.getSingleBatteryUnderVoltageAlarm() != -999999) put.addColumn(cfBytes, Bytes.toBytes("singleBatteryUnderVoltageAlarm"), Bytes.toBytes(String.valueOf(vehicleDataObj.getSingleBatteryUnderVoltageAlarm())));
		if (vehicleDataObj.getMaxVoltageBatterySubSystemNum() != -999999) put.addColumn(cfBytes, Bytes.toBytes("maxVoltageBatterySubSystemNum"), Bytes.toBytes(String.valueOf(vehicleDataObj.getMaxVoltageBatterySubSystemNum())));
		if (vehicleDataObj.getMinTemperatureProbe() != -999999) put.addColumn(cfBytes, Bytes.toBytes("minTemperatureProbe"), Bytes.toBytes(String.valueOf(vehicleDataObj.getMinTemperatureProbe())));
		if (vehicleDataObj.getDriveMotorNum() != -999999) put.addColumn(cfBytes, Bytes.toBytes("driveMotorNum"), Bytes.toBytes(String.valueOf(vehicleDataObj.getDriveMotorNum())));
		if (vehicleDataObj.getTotalVoltage() != -999999) put.addColumn(cfBytes, Bytes.toBytes("totalVoltage"), Bytes.toBytes(String.valueOf(vehicleDataObj.getTotalVoltage())));
		if (vehicleDataObj.getTemperatureDifferenceAlarm() != -999999) put.addColumn(cfBytes, Bytes.toBytes("temperatureDifferenceAlarm"), Bytes.toBytes(String.valueOf(vehicleDataObj.getTemperatureDifferenceAlarm())));
		if (vehicleDataObj.getMaxAlarmLevel() != -999999) put.addColumn(cfBytes, Bytes.toBytes("maxAlarmLevel"), Bytes.toBytes(String.valueOf(vehicleDataObj.getMaxAlarmLevel())));
		if (vehicleDataObj.getStatus() != -999999) put.addColumn(cfBytes, Bytes.toBytes("status"), Bytes.toBytes(String.valueOf(vehicleDataObj.getStatus())));
		if (vehicleDataObj.getGeerPosition() != -999999) put.addColumn(cfBytes, Bytes.toBytes("geerPosition"), Bytes.toBytes(String.valueOf(vehicleDataObj.getGeerPosition())));
		if (vehicleDataObj.getAverageEnergyConsumption() != -999999) put.addColumn(cfBytes, Bytes.toBytes("averageEnergyConsumption"), Bytes.toBytes(String.valueOf(vehicleDataObj.getAverageEnergyConsumption())));
		if (vehicleDataObj.getMinVoltageBattery() != -999999) put.addColumn(cfBytes, Bytes.toBytes("minVoltageBattery"), Bytes.toBytes(String.valueOf(vehicleDataObj.getMinVoltageBattery())));
		if (vehicleDataObj.getGeerStatus() != -999999) put.addColumn(cfBytes, Bytes.toBytes("geerStatus"), Bytes.toBytes(String.valueOf(vehicleDataObj.getGeerStatus())));
		if (vehicleDataObj.getControllerInputVoltage() != -999999) put.addColumn(cfBytes, Bytes.toBytes("controllerInputVoltage"), Bytes.toBytes(String.valueOf(vehicleDataObj.getControllerInputVoltage())));
		if (vehicleDataObj.getControllerTemperature() != -999999) put.addColumn(cfBytes, Bytes.toBytes("controllerTemperature"), Bytes.toBytes(String.valueOf(vehicleDataObj.getControllerTemperature())));
		if (vehicleDataObj.getRevolutionSpeed() != -999999) put.addColumn(cfBytes, Bytes.toBytes("revolutionSpeed"), Bytes.toBytes(String.valueOf(vehicleDataObj.getRevolutionSpeed())));
		if (vehicleDataObj.getNum() != -999999) put.addColumn(cfBytes, Bytes.toBytes("num"), Bytes.toBytes(String.valueOf(vehicleDataObj.getNum())));
		if (vehicleDataObj.getControllerDcBusCurrent() != -999999) put.addColumn(cfBytes, Bytes.toBytes("controllerDcBusCurrent"), Bytes.toBytes(String.valueOf(vehicleDataObj.getControllerDcBusCurrent())));
		if (vehicleDataObj.getTemperature() != -999999) put.addColumn(cfBytes, Bytes.toBytes("temperature"), Bytes.toBytes(String.valueOf(vehicleDataObj.getTemperature())));
		if (vehicleDataObj.getTorque() != -999999) put.addColumn(cfBytes, Bytes.toBytes("torque"), Bytes.toBytes(String.valueOf(vehicleDataObj.getTorque())));
		if (vehicleDataObj.getState() != -999999) put.addColumn(cfBytes, Bytes.toBytes("state"), Bytes.toBytes(String.valueOf(vehicleDataObj.getState())));
		if (vehicleDataObj.getMinVoltageBatteryNum() != -999999) put.addColumn(cfBytes, Bytes.toBytes("minVoltageBatteryNum"), Bytes.toBytes(String.valueOf(vehicleDataObj.getMinVoltageBatteryNum())));
		if (!vehicleDataObj.getValidGps().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("validGps"), Bytes.toBytes(vehicleDataObj.getValidGps()));
		if (!vehicleDataObj.getEngineFaultCodes().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("engineFaultCodes"), Bytes.toBytes(vehicleDataObj.getEngineFaultCodes()));
		if (vehicleDataObj.getMinTemperatureValue() != -999999) put.addColumn(cfBytes, Bytes.toBytes("minTemperatureValue"), Bytes.toBytes(String.valueOf(vehicleDataObj.getMinTemperatureValue())));
		if (vehicleDataObj.getChargeStatus() != -999999) put.addColumn(cfBytes, Bytes.toBytes("chargeStatus"), Bytes.toBytes(String.valueOf(vehicleDataObj.getChargeStatus())));
		if (!vehicleDataObj.getIgnitionTime().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("ignitionTime"), Bytes.toBytes(vehicleDataObj.getIgnitionTime()));
		if (vehicleDataObj.getTotalOdometer() != -999999) put.addColumn(cfBytes, Bytes.toBytes("totalOdometer"), Bytes.toBytes(String.valueOf(vehicleDataObj.getTotalOdometer())));
		if (vehicleDataObj.getAlti() != -999999) put.addColumn(cfBytes, Bytes.toBytes("alti"), Bytes.toBytes(String.valueOf(vehicleDataObj.getAlti())));
		if (vehicleDataObj.getSpeed() != -999999) put.addColumn(cfBytes, Bytes.toBytes("speed"), Bytes.toBytes(String.valueOf(vehicleDataObj.getSpeed())));
		if (vehicleDataObj.getSocHighAlarm() != -999999) put.addColumn(cfBytes, Bytes.toBytes("socHighAlarm"), Bytes.toBytes(String.valueOf(vehicleDataObj.getSocHighAlarm())));
		if (vehicleDataObj.getVehicleStorageDeviceUndervoltageAlarm() != -999999) put.addColumn(cfBytes, Bytes.toBytes("vehicleStorageDeviceUndervoltageAlarm"), Bytes.toBytes(String.valueOf(vehicleDataObj.getVehicleStorageDeviceUndervoltageAlarm())));
		if (vehicleDataObj.getTotalCurrent() != -999999) put.addColumn(cfBytes, Bytes.toBytes("totalCurrent"), Bytes.toBytes(String.valueOf(vehicleDataObj.getTotalCurrent())));
		if (vehicleDataObj.getBatteryAlarm() != -999999) put.addColumn(cfBytes, Bytes.toBytes("batteryAlarm"), Bytes.toBytes(String.valueOf(vehicleDataObj.getBatteryAlarm())));
		if (vehicleDataObj.getRechargeableStorageDeviceMismatchAlarm() != -999999) put.addColumn(cfBytes, Bytes.toBytes("rechargeableStorageDeviceMismatchAlarm"), Bytes.toBytes(String.valueOf(vehicleDataObj.getRechargeableStorageDeviceMismatchAlarm())));
		if (vehicleDataObj.getIsHistoryPoi() != -999999) put.addColumn(cfBytes, Bytes.toBytes("isHistoryPoi"), Bytes.toBytes(String.valueOf(vehicleDataObj.getIsHistoryPoi())));
		if (vehicleDataObj.getVehiclePureDeviceTypeOvercharge() != -999999) put.addColumn(cfBytes, Bytes.toBytes("vehiclePureDeviceTypeOvercharge"), Bytes.toBytes(String.valueOf(vehicleDataObj.getVehiclePureDeviceTypeOvercharge())));
		if (vehicleDataObj.getMaxVoltageBattery() != -999999) put.addColumn(cfBytes, Bytes.toBytes("maxVoltageBattery"), Bytes.toBytes(String.valueOf(vehicleDataObj.getMaxVoltageBattery())));
		if (vehicleDataObj.getDcdcTemperatureAlarm() != -999999) put.addColumn(cfBytes, Bytes.toBytes("dcdcTemperatureAlarm"), Bytes.toBytes(String.valueOf(vehicleDataObj.getDcdcTemperatureAlarm())));
		if (!vehicleDataObj.getIsValidGps().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("isValidGps"), Bytes.toBytes(vehicleDataObj.getIsValidGps()));
		if (!vehicleDataObj.getLastUpdatedTime().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("lastUpdatedTime"), Bytes.toBytes(vehicleDataObj.getLastUpdatedTime()));
		if (vehicleDataObj.getDriveMotorControllerTemperatureAlarm() != -999999) put.addColumn(cfBytes, Bytes.toBytes("driveMotorControllerTemperatureAlarm"), Bytes.toBytes(String.valueOf(vehicleDataObj.getDriveMotorControllerTemperatureAlarm())));
		if (!vehicleDataObj.getProbeTemperatures().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("probeTemperatures"), Bytes.toBytes(vehicleDataObj.getProbeTemperatures()));
		if (vehicleDataObj.getChargeTemperatureProbeNum() != -999999) put.addColumn(cfBytes, Bytes.toBytes("chargeTemperatureProbeNum"), Bytes.toBytes(String.valueOf(vehicleDataObj.getChargeTemperatureProbeNum())));
		if (vehicleDataObj.getIgniteCumulativeMileage() != -999999) put.addColumn(cfBytes, Bytes.toBytes("igniteCumulativeMileage"), Bytes.toBytes(String.valueOf(vehicleDataObj.getIgniteCumulativeMileage())));
		if (vehicleDataObj.getDcStatus() != -999999) put.addColumn(cfBytes, Bytes.toBytes("dcStatus"), Bytes.toBytes(String.valueOf(vehicleDataObj.getDcStatus())));
		if (!vehicleDataObj.getRepay().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("repay"), Bytes.toBytes(vehicleDataObj.getRepay()));
		if (vehicleDataObj.getMaxTemperatureSubSystemNum() != -999999) put.addColumn(cfBytes, Bytes.toBytes("maxTemperatureSubSystemNum"), Bytes.toBytes(String.valueOf(vehicleDataObj.getMaxTemperatureSubSystemNum())));
		if (vehicleDataObj.getCarStatus() != -999999) put.addColumn(cfBytes, Bytes.toBytes("carStatus"), Bytes.toBytes(String.valueOf(vehicleDataObj.getCarStatus())));
		if (vehicleDataObj.getMinVoltageBatterySubSystemNum() != -999999) put.addColumn(cfBytes, Bytes.toBytes("minVoltageBatterySubSystemNum"), Bytes.toBytes(String.valueOf(vehicleDataObj.getMinVoltageBatterySubSystemNum())));
		if (vehicleDataObj.getHeading() != -999999) put.addColumn(cfBytes, Bytes.toBytes("heading"), Bytes.toBytes(String.valueOf(vehicleDataObj.getHeading())));
		if (vehicleDataObj.getDriveMotorFaultCount() != -999999) put.addColumn(cfBytes, Bytes.toBytes("driveMotorFaultCount"), Bytes.toBytes(String.valueOf(vehicleDataObj.getDriveMotorFaultCount())));
		if (!vehicleDataObj.getTuid().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("tuid"), Bytes.toBytes(vehicleDataObj.getTuid()));
		if (vehicleDataObj.getEnergyRecoveryStatus() != -999999) put.addColumn(cfBytes, Bytes.toBytes("energyRecoveryStatus"), Bytes.toBytes(String.valueOf(vehicleDataObj.getEnergyRecoveryStatus())));
		if (vehicleDataObj.getFireStatus() != -999999) put.addColumn(cfBytes, Bytes.toBytes("fireStatus"), Bytes.toBytes(String.valueOf(vehicleDataObj.getFireStatus())));
		if (!vehicleDataObj.getTargetType().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("targetType"), Bytes.toBytes(vehicleDataObj.getTargetType()));
		if (vehicleDataObj.getMaxTemperatureProbe() != -999999) put.addColumn(cfBytes, Bytes.toBytes("maxTemperatureProbe"), Bytes.toBytes(String.valueOf(vehicleDataObj.getMaxTemperatureProbe())));
		if (!vehicleDataObj.getRechargeableStorageDevicesFaultCodes().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("rechargeableStorageDevicesFaultCodes"), Bytes.toBytes(vehicleDataObj.getRechargeableStorageDevicesFaultCodes()));
		if (vehicleDataObj.getCarMode() != -999999) put.addColumn(cfBytes, Bytes.toBytes("carMode"), Bytes.toBytes(String.valueOf(vehicleDataObj.getCarMode())));
		if (vehicleDataObj.getHighVoltageInterlockStateAlarm() != -999999) put.addColumn(cfBytes, Bytes.toBytes("highVoltageInterlockStateAlarm"), Bytes.toBytes(String.valueOf(vehicleDataObj.getHighVoltageInterlockStateAlarm())));
		if (vehicleDataObj.getInsulationAlarm() != -999999) put.addColumn(cfBytes, Bytes.toBytes("insulationAlarm"), Bytes.toBytes(String.valueOf(vehicleDataObj.getInsulationAlarm())));
		if (vehicleDataObj.getMileageInformation() != -999999) put.addColumn(cfBytes, Bytes.toBytes("mileageInformation"), Bytes.toBytes(String.valueOf(vehicleDataObj.getMileageInformation())));
		if (vehicleDataObj.getMaxTemperatureValue() != -999999) put.addColumn(cfBytes, Bytes.toBytes("maxTemperatureValue"), Bytes.toBytes(String.valueOf(vehicleDataObj.getMaxTemperatureValue())));
		if (vehicleDataObj.getOtherFaultCodes().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("otherFaultCodes"), Bytes.toBytes(vehicleDataObj.getOtherFaultCodes()));
		if (vehicleDataObj.getRemainPower() != -999999) put.addColumn(cfBytes, Bytes.toBytes("remainPower"), Bytes.toBytes(String.valueOf(vehicleDataObj.getRemainPower())));
		if (vehicleDataObj.getInsulateResistance() != -999999) put.addColumn(cfBytes, Bytes.toBytes("insulateResistance"), Bytes.toBytes(String.valueOf(vehicleDataObj.getInsulateResistance())));
		if (vehicleDataObj.getBatteryLowTemperatureHeater() != -999999) put.addColumn(cfBytes, Bytes.toBytes("batteryLowTemperatureHeater"), Bytes.toBytes(String.valueOf(vehicleDataObj.getBatteryLowTemperatureHeater())));
		if (!vehicleDataObj.getFuelConsumption100km().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("fuelConsumption100km"), Bytes.toBytes(vehicleDataObj.getFuelConsumption100km()));
		if (!vehicleDataObj.getFuelConsumption().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("fuelConsumption"), Bytes.toBytes(vehicleDataObj.getFuelConsumption()));
		if (!vehicleDataObj.getEngineSpeed().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("engineSpeed"), Bytes.toBytes(vehicleDataObj.getEngineSpeed()));
		if (!vehicleDataObj.getEngineStatus().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("engineStatus"), Bytes.toBytes(vehicleDataObj.getEngineStatus()));
		if (vehicleDataObj.getTrunk() != -999999) put.addColumn(cfBytes, Bytes.toBytes("trunk"), Bytes.toBytes(String.valueOf(vehicleDataObj.getTrunk())));
		if (vehicleDataObj.getLowBeam() != -999999) put.addColumn(cfBytes, Bytes.toBytes("lowBeam"), Bytes.toBytes(String.valueOf(vehicleDataObj.getLowBeam())));
		if (!vehicleDataObj.getTriggerLatchOverheatProtect().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("triggerLatchOverheatProtect"), Bytes.toBytes(vehicleDataObj.getTriggerLatchOverheatProtect()));
		if (vehicleDataObj.getTurnLndicatorRight() != -999999) put.addColumn(cfBytes, Bytes.toBytes("turnLndicatorRight"), Bytes.toBytes(String.valueOf(vehicleDataObj.getTurnLndicatorRight())));
		if (vehicleDataObj.getHighBeam() != -999999) put.addColumn(cfBytes, Bytes.toBytes("highBeam"), Bytes.toBytes(String.valueOf(vehicleDataObj.getHighBeam())));
		if (vehicleDataObj.getTurnLndicatorLeft() != -999999) put.addColumn(cfBytes, Bytes.toBytes("turnLndicatorLeft"), Bytes.toBytes(String.valueOf(vehicleDataObj.getTurnLndicatorLeft())));
		if (vehicleDataObj.getBcuSwVers() != -999999) put.addColumn(cfBytes, Bytes.toBytes("bcuSwVers"), Bytes.toBytes(String.valueOf(vehicleDataObj.getBcuSwVers())));
		if (vehicleDataObj.getBcuHwVers() != -999999) put.addColumn(cfBytes, Bytes.toBytes("bcuHwVers"), Bytes.toBytes(String.valueOf(vehicleDataObj.getBcuHwVers())));
		if (vehicleDataObj.getBcuOperMod() != -999999) put.addColumn(cfBytes, Bytes.toBytes("bcuOperMod"), Bytes.toBytes(String.valueOf(vehicleDataObj.getBcuOperMod())));
		if (vehicleDataObj.getChrgEndReason() != -999999) put.addColumn(cfBytes, Bytes.toBytes("chrgEndReason"), Bytes.toBytes(String.valueOf(vehicleDataObj.getChrgEndReason())));
		if (!vehicleDataObj.getBCURegenEngDisp().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("BCURegenEngDisp"), Bytes.toBytes(vehicleDataObj.getBCURegenEngDisp()));
		if (vehicleDataObj.getBCURegenCpDisp() != -999999) put.addColumn(cfBytes, Bytes.toBytes("BCURegenCpDisp"), Bytes.toBytes(String.valueOf(vehicleDataObj.getBCURegenCpDisp())));
		if (vehicleDataObj.getBcuChrgMod() != -999999) put.addColumn(cfBytes, Bytes.toBytes("bcuChrgMod"), Bytes.toBytes(String.valueOf(vehicleDataObj.getBcuChrgMod())));
		if (vehicleDataObj.getBatteryChargeStatus() != -999999) put.addColumn(cfBytes, Bytes.toBytes("batteryChargeStatus"), Bytes.toBytes(String.valueOf(vehicleDataObj.getBatteryChargeStatus())));
		if (!vehicleDataObj.getBcuFaultCodes().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("BcuFaultCodes"), Bytes.toBytes(vehicleDataObj.getBcuFaultCodes()));
		if (vehicleDataObj.getBcuFltRnk() != -999999) put.addColumn(cfBytes, Bytes.toBytes("bcuFltRnk"), Bytes.toBytes(String.valueOf(vehicleDataObj.getBcuFltRnk())));
		if (!vehicleDataObj.getBattPoleTOver().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("battPoleTOver"), Bytes.toBytes(vehicleDataObj.getBattPoleTOver()));
		if (vehicleDataObj.getBcuSOH() != -999999) put.addColumn(cfBytes, Bytes.toBytes("bcuSOH"), Bytes.toBytes(String.valueOf(vehicleDataObj.getBcuSOH())));
		if (vehicleDataObj.getBattIntrHeatActive() != -999999) put.addColumn(cfBytes, Bytes.toBytes("battIntrHeatActive"), Bytes.toBytes(String.valueOf(vehicleDataObj.getBattIntrHeatActive())));
		if (vehicleDataObj.getBattIntrHeatReq() != -999999) put.addColumn(cfBytes, Bytes.toBytes("battIntrHeatReq"), Bytes.toBytes(String.valueOf(vehicleDataObj.getBattIntrHeatReq())));
		if (!vehicleDataObj.getBCUBattTarT().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("BCUBattTarT"), Bytes.toBytes(vehicleDataObj.getBCUBattTarT()));
		if (vehicleDataObj.getBattExtHeatReq() != -999999) put.addColumn(cfBytes, Bytes.toBytes("battExtHeatReq"), Bytes.toBytes(String.valueOf(vehicleDataObj.getBattExtHeatReq())));
		if (!vehicleDataObj.getBCUMaxChrgPwrLongT().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("BCUMaxChrgPwrLongT"), Bytes.toBytes(vehicleDataObj.getBCUMaxChrgPwrLongT()));
		if (!vehicleDataObj.getBCUMaxDchaPwrLongT().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("BCUMaxDchaPwrLongT"), Bytes.toBytes(vehicleDataObj.getBCUMaxDchaPwrLongT()));
		if (!vehicleDataObj.getBCUTotalRegenEngDisp().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("BCUTotalRegenEngDisp"), Bytes.toBytes(vehicleDataObj.getBCUTotalRegenEngDisp()));
		if (!vehicleDataObj.getBCUTotalRegenCpDisp().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("BCUTotalRegenCpDisp"), Bytes.toBytes(vehicleDataObj.getBCUTotalRegenCpDisp()));
		if (vehicleDataObj.getDcdcFltRnk() != -999999) put.addColumn(cfBytes, Bytes.toBytes("dcdcFltRnk"), Bytes.toBytes(String.valueOf(vehicleDataObj.getDcdcFltRnk())));
		if (!vehicleDataObj.getDcdcFaultCode().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("DcdcFaultCode"), Bytes.toBytes(vehicleDataObj.getDcdcFaultCode()));
		if (vehicleDataObj.getDcdcOutpCrrt() != -999999) put.addColumn(cfBytes, Bytes.toBytes("dcdcOutpCrrt"), Bytes.toBytes(String.valueOf(vehicleDataObj.getDcdcOutpCrrt())));
		if (vehicleDataObj.getDcdcOutpU() != -999999) put.addColumn(cfBytes, Bytes.toBytes("dcdcOutpU"), Bytes.toBytes(String.valueOf(vehicleDataObj.getDcdcOutpU())));
		if (vehicleDataObj.getDcdcAvlOutpPwr() != -999999) put.addColumn(cfBytes, Bytes.toBytes("dcdcAvlOutpPwr"), Bytes.toBytes(String.valueOf(vehicleDataObj.getDcdcAvlOutpPwr())));
		if (!vehicleDataObj.getAbsActiveStatus().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("absActiveStatus"), Bytes.toBytes(vehicleDataObj.getAbsActiveStatus()));
		if (!vehicleDataObj.getAbsStatus().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("absStatus"), Bytes.toBytes(vehicleDataObj.getAbsStatus()));
		if (!vehicleDataObj.getVcuBrkErr().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("VcuBrkErr"), Bytes.toBytes(vehicleDataObj.getVcuBrkErr()));
		if (!vehicleDataObj.getEPB_AchievedClampForce().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("EPB_AchievedClampForce"), Bytes.toBytes(vehicleDataObj.getEPB_AchievedClampForce()));
		if (!vehicleDataObj.getEpbSwitchPosition().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("epbSwitchPosition"), Bytes.toBytes(vehicleDataObj.getEpbSwitchPosition()));
		if (!vehicleDataObj.getEpbStatus().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("epbStatus"), Bytes.toBytes(vehicleDataObj.getEpbStatus()));
		if (!vehicleDataObj.getEspActiveStatus().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("espActiveStatus"), Bytes.toBytes(vehicleDataObj.getEspActiveStatus()));
		if (!vehicleDataObj.getEspFunctionStatus().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("espFunctionStatus"), Bytes.toBytes(vehicleDataObj.getEspFunctionStatus()));
		if (!vehicleDataObj.getESP_TCSFailStatus().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("ESP_TCSFailStatus"), Bytes.toBytes(vehicleDataObj.getESP_TCSFailStatus()));
		if (!vehicleDataObj.getHhcActive().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("hhcActive"), Bytes.toBytes(vehicleDataObj.getHhcActive()));
		if (!vehicleDataObj.getTcsActive().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("tcsActive"), Bytes.toBytes(vehicleDataObj.getTcsActive()));
		if (!vehicleDataObj.getEspMasterCylinderBrakePressure().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("espMasterCylinderBrakePressure"), Bytes.toBytes(vehicleDataObj.getEspMasterCylinderBrakePressure()));
		if (!vehicleDataObj.getESP_MasterCylinderBrakePressureValid().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("ESP_MasterCylinderBrakePressureValid"), Bytes.toBytes(vehicleDataObj.getESP_MasterCylinderBrakePressureValid()));
		if (!vehicleDataObj.getEspTorqSensorStatus().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("espTorqSensorStatus"), Bytes.toBytes(vehicleDataObj.getEspTorqSensorStatus()));
		if (!vehicleDataObj.getEPS_EPSFailed().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("EPS_EPSFailed"), Bytes.toBytes(vehicleDataObj.getEPS_EPSFailed()));
		if (!vehicleDataObj.getSasFailure().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("sasFailure"), Bytes.toBytes(vehicleDataObj.getSasFailure()));
		if (!vehicleDataObj.getSasSteeringAngleSpeed().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("sasSteeringAngleSpeed"), Bytes.toBytes(vehicleDataObj.getSasSteeringAngleSpeed()));
		if (!vehicleDataObj.getSasSteeringAngle().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("sasSteeringAngle"), Bytes.toBytes(vehicleDataObj.getSasSteeringAngle()));
		if (!vehicleDataObj.getSasSteeringAngleValid().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("sasSteeringAngleValid"), Bytes.toBytes(vehicleDataObj.getSasSteeringAngleValid()));
		if (!vehicleDataObj.getEspSteeringTorque().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("espSteeringTorque"), Bytes.toBytes(vehicleDataObj.getEspSteeringTorque()));
		if (vehicleDataObj.getAcReq() != -999999) put.addColumn(cfBytes, Bytes.toBytes("acReq"), Bytes.toBytes(String.valueOf(vehicleDataObj.getAcReq())));
		if (vehicleDataObj.getAcSystemFailure() != -999999) put.addColumn(cfBytes, Bytes.toBytes("acSystemFailure"), Bytes.toBytes(String.valueOf(vehicleDataObj.getAcSystemFailure())));
		if (vehicleDataObj.getPtcPwrAct() != -999999) put.addColumn(cfBytes, Bytes.toBytes("ptcPwrAct"), Bytes.toBytes(String.valueOf(vehicleDataObj.getPtcPwrAct())));
		if (vehicleDataObj.getPlasmaStatus() != -999999) put.addColumn(cfBytes, Bytes.toBytes("plasmaStatus"), Bytes.toBytes(String.valueOf(vehicleDataObj.getPlasmaStatus())));
		if (vehicleDataObj.getBattInTemperature() != -999999) put.addColumn(cfBytes, Bytes.toBytes("battInTemperature"), Bytes.toBytes(String.valueOf(vehicleDataObj.getBattInTemperature())));
		if (!vehicleDataObj.getBattWarmLoopSts().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("battWarmLoopSts"), Bytes.toBytes(vehicleDataObj.getBattWarmLoopSts()));
		if (!vehicleDataObj.getBattCoolngLoopSts().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("battCoolngLoopSts"), Bytes.toBytes(vehicleDataObj.getBattCoolngLoopSts()));
		if (!vehicleDataObj.getBattCoolActv().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("battCoolActv"), Bytes.toBytes(vehicleDataObj.getBattCoolActv()));
		if (vehicleDataObj.getMotorOutTemperature() != -999999) put.addColumn(cfBytes, Bytes.toBytes("motorOutTemperature"), Bytes.toBytes(String.valueOf(vehicleDataObj.getMotorOutTemperature())));
		if (!vehicleDataObj.getPowerStatusFeedBack().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("powerStatusFeedBack"), Bytes.toBytes(String.valueOf(vehicleDataObj.getPowerStatusFeedBack())));
		if (vehicleDataObj.getAC_RearDefrosterSwitch() != -999999) put.addColumn(cfBytes, Bytes.toBytes("AC_RearDefrosterSwitch"), Bytes.toBytes(String.valueOf(vehicleDataObj.getAC_RearDefrosterSwitch())));
		if (vehicleDataObj.getRearFoglamp() != -999999) put.addColumn(cfBytes, Bytes.toBytes("rearFoglamp"), Bytes.toBytes(String.valueOf(vehicleDataObj.getRearFoglamp())));
		if (vehicleDataObj.getDriverDoorLock() != -999999) put.addColumn(cfBytes, Bytes.toBytes("driverDoorLock"), Bytes.toBytes(String.valueOf(vehicleDataObj.getDriverDoorLock())));
		if (vehicleDataObj.getAcDriverReqTemp() != -999999) put.addColumn(cfBytes, Bytes.toBytes("acDriverReqTemp"), Bytes.toBytes(String.valueOf(vehicleDataObj.getAcDriverReqTemp())));
		if (vehicleDataObj.getKeyAlarm() != -999999) put.addColumn(cfBytes, Bytes.toBytes("keyAlarm"), Bytes.toBytes(String.valueOf(vehicleDataObj.getKeyAlarm())));
		if (vehicleDataObj.getAirCleanStsRemind() != -999999) put.addColumn(cfBytes, Bytes.toBytes("airCleanStsRemind"), Bytes.toBytes(String.valueOf(vehicleDataObj.getAirCleanStsRemind())));
		if (vehicleDataObj.getRecycleType() != -999999) put.addColumn(cfBytes, Bytes.toBytes("recycleType"), Bytes.toBytes(String.valueOf(vehicleDataObj.getRecycleType())));
		if (!vehicleDataObj.getStartControlsignal().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("startControlsignal"), Bytes.toBytes(vehicleDataObj.getStartControlsignal()));
		if (vehicleDataObj.getAirBagWarningLamp() != -999999) put.addColumn(cfBytes, Bytes.toBytes("airBagWarningLamp"), Bytes.toBytes(String.valueOf(vehicleDataObj.getAirBagWarningLamp())));
		if (vehicleDataObj.getFrontDefrosterSwitch() != -999999) put.addColumn(cfBytes, Bytes.toBytes("frontDefrosterSwitch"), Bytes.toBytes(String.valueOf(vehicleDataObj.getFrontDefrosterSwitch())));
		if (!vehicleDataObj.getFrontBlowType().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("frontBlowType"), Bytes.toBytes(vehicleDataObj.getFrontBlowType()));
		if (vehicleDataObj.getFrontReqWindLevel() != -999999) put.addColumn(cfBytes, Bytes.toBytes("frontReqWindLevel"), Bytes.toBytes(String.valueOf(vehicleDataObj.getFrontReqWindLevel())));
		if (!vehicleDataObj.getBcmFrontWiperStatus().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("bcmFrontWiperStatus"), Bytes.toBytes(vehicleDataObj.getBcmFrontWiperStatus()));
		if (!vehicleDataObj.getTmsPwrAct().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("tmsPwrAct"), Bytes.toBytes(vehicleDataObj.getTmsPwrAct()));
		if (vehicleDataObj.getKeyUndetectedAlarmSign() != -999999) put.addColumn(cfBytes, Bytes.toBytes("keyUndetectedAlarmSign"), Bytes.toBytes(String.valueOf(vehicleDataObj.getKeyUndetectedAlarmSign())));
		if (!vehicleDataObj.getPositionLamp().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("positionLamp"), Bytes.toBytes(vehicleDataObj.getPositionLamp()));
		if (vehicleDataObj.getDriverReqTempModel() != -999999) put.addColumn(cfBytes, Bytes.toBytes("driverReqTempModel"), Bytes.toBytes(String.valueOf(vehicleDataObj.getDriverReqTempModel())));
		if (vehicleDataObj.getTurnLightSwitchSts() != -999999) put.addColumn(cfBytes, Bytes.toBytes("turnLightSwitchSts"), Bytes.toBytes(String.valueOf(vehicleDataObj.getTurnLightSwitchSts())));
		if (vehicleDataObj.getAutoHeadlightStatus() != -999999) put.addColumn(cfBytes, Bytes.toBytes("autoHeadlightStatus"), Bytes.toBytes(String.valueOf(vehicleDataObj.getAutoHeadlightStatus())));
		if (vehicleDataObj.getDriverDoor() != -999999) put.addColumn(cfBytes, Bytes.toBytes("driverDoor"), Bytes.toBytes(String.valueOf(vehicleDataObj.getDriverDoor())));
		if (!vehicleDataObj.getIpuFaultCodes().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("IpuFaultCodes"), Bytes.toBytes(vehicleDataObj.getIpuFaultCodes()));
		if (vehicleDataObj.getFrntIpuFltRnk() != -999999) put.addColumn(cfBytes, Bytes.toBytes("frntIpuFltRnk"), Bytes.toBytes(String.valueOf(vehicleDataObj.getFrntIpuFltRnk())));
		if (!vehicleDataObj.getFrontIpuSwVers().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("frontIpuSwVers"), Bytes.toBytes(vehicleDataObj.getFrontIpuSwVers()));
		if (vehicleDataObj.getFrontIpuHwVers() != -999999) put.addColumn(cfBytes, Bytes.toBytes("frontIpuHwVers"), Bytes.toBytes(String.valueOf(vehicleDataObj.getFrontIpuHwVers())));
		if (vehicleDataObj.getFrntMotTqLongTermMax() != -999999) put.addColumn(cfBytes, Bytes.toBytes("frntMotTqLongTermMax"), Bytes.toBytes(String.valueOf(vehicleDataObj.getFrntMotTqLongTermMax())));
		if (vehicleDataObj.getFrntMotTqLongTermMin() != -999999) put.addColumn(cfBytes, Bytes.toBytes("frntMotTqLongTermMin"), Bytes.toBytes(String.valueOf(vehicleDataObj.getFrntMotTqLongTermMin())));
		if (vehicleDataObj.getCpvValue() != -999999) put.addColumn(cfBytes, Bytes.toBytes("cpvValue"), Bytes.toBytes(String.valueOf(vehicleDataObj.getCpvValue())));
		if (vehicleDataObj.getObcChrgSts() != -999999) put.addColumn(cfBytes, Bytes.toBytes("obcChrgSts"), Bytes.toBytes(String.valueOf(vehicleDataObj.getObcChrgSts())));
		if (!vehicleDataObj.getObcFltRnk().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("obcFltRnk"), Bytes.toBytes(vehicleDataObj.getObcFltRnk()));
		if (vehicleDataObj.getObcChrgInpAcI() != -999999) put.addColumn(cfBytes, Bytes.toBytes("obcChrgInpAcI"), Bytes.toBytes(String.valueOf(vehicleDataObj.getObcChrgInpAcI())));
		if (vehicleDataObj.getObcChrgInpAcU() != -999999) put.addColumn(cfBytes, Bytes.toBytes("obcChrgInpAcU"), Bytes.toBytes(String.valueOf(vehicleDataObj.getObcChrgInpAcU())));
		if (vehicleDataObj.getObcChrgDcI() != -999999) put.addColumn(cfBytes, Bytes.toBytes("obcChrgDcI"), Bytes.toBytes(String.valueOf(vehicleDataObj.getObcChrgDcI())));
		if (vehicleDataObj.getObcChrgDcU() != -999999) put.addColumn(cfBytes, Bytes.toBytes("obcChrgDcU"), Bytes.toBytes(String.valueOf(vehicleDataObj.getObcChrgDcU())));
		if (vehicleDataObj.getObcTemperature() != -999999) put.addColumn(cfBytes, Bytes.toBytes("obcTemperature"), Bytes.toBytes(String.valueOf(vehicleDataObj.getObcTemperature())));
		if (vehicleDataObj.getObcMaxChrgOutpPwrAvl() != -999999) put.addColumn(cfBytes, Bytes.toBytes("obcMaxChrgOutpPwrAvl"), Bytes.toBytes(String.valueOf(vehicleDataObj.getObcMaxChrgOutpPwrAvl())));
		if (vehicleDataObj.getPassengerBuckleSwitch() != -999999) put.addColumn(cfBytes, Bytes.toBytes("passengerBuckleSwitch"), Bytes.toBytes(String.valueOf(vehicleDataObj.getPassengerBuckleSwitch())));
		if (!vehicleDataObj.getCrashlfo().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("crashlfo"), Bytes.toBytes(vehicleDataObj.getCrashlfo()));
		if (vehicleDataObj.getDriverBuckleSwitch() != -999999) put.addColumn(cfBytes, Bytes.toBytes("driverBuckleSwitch"), Bytes.toBytes(String.valueOf(vehicleDataObj.getDriverBuckleSwitch())));
		if (!vehicleDataObj.getEngineStartHibit().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("engineStartHibit"), Bytes.toBytes(vehicleDataObj.getEngineStartHibit()));
		if (!vehicleDataObj.getLockCommand().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("lockCommand"), Bytes.toBytes(vehicleDataObj.getLockCommand()));
		if (!vehicleDataObj.getSearchCarReq().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("searchCarReq"), Bytes.toBytes(vehicleDataObj.getSearchCarReq()));
		if (!vehicleDataObj.getAcTempValueReq().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("acTempValueReq"), Bytes.toBytes(vehicleDataObj.getAcTempValueReq()));
		if (!vehicleDataObj.getVcuFaultCode().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("VcuFaultCode"), Bytes.toBytes(vehicleDataObj.getVcuFaultCode()));
		if (!vehicleDataObj.getVcuErrAmnt().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("vcuErrAmnt"), Bytes.toBytes(vehicleDataObj.getVcuErrAmnt()));
		if (vehicleDataObj.getVcuSwVers() != -999999) put.addColumn(cfBytes, Bytes.toBytes("vcuSwVers"), Bytes.toBytes(String.valueOf(vehicleDataObj.getVcuSwVers())));
		if (vehicleDataObj.getVcuHwVers() != -999999) put.addColumn(cfBytes, Bytes.toBytes("vcuHwVers"), Bytes.toBytes(String.valueOf(vehicleDataObj.getVcuHwVers())));
		if (!vehicleDataObj.getLowSpdWarnStatus().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("lowSpdWarnStatus"), Bytes.toBytes(vehicleDataObj.getLowSpdWarnStatus()));
		if (vehicleDataObj.getLowBattChrgRqe() != -999999) put.addColumn(cfBytes, Bytes.toBytes("lowBattChrgRqe"), Bytes.toBytes(String.valueOf(vehicleDataObj.getLowBattChrgRqe())));
		if (!vehicleDataObj.getLowBattChrgSts().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("lowBattChrgSts"), Bytes.toBytes(vehicleDataObj.getLowBattChrgSts()));
		if (vehicleDataObj.getLowBattU() != -999999) put.addColumn(cfBytes, Bytes.toBytes("lowBattU"), Bytes.toBytes(String.valueOf(vehicleDataObj.getLowBattU())));
		if (vehicleDataObj.getHandlebrakeStatus() != -999999) put.addColumn(cfBytes, Bytes.toBytes("handlebrakeStatus"), Bytes.toBytes(String.valueOf(vehicleDataObj.getHandlebrakeStatus())));
		if (!vehicleDataObj.getShiftPositionValid().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("shiftPositionValid"), Bytes.toBytes(vehicleDataObj.getShiftPositionValid()));
		if (!vehicleDataObj.getAccPedalValid().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("accPedalValid"), Bytes.toBytes(vehicleDataObj.getAccPedalValid()));
		if (vehicleDataObj.getDriveMode() != -999999) put.addColumn(cfBytes, Bytes.toBytes("driveMode"), Bytes.toBytes(String.valueOf(vehicleDataObj.getDriveMode())));
		if (vehicleDataObj.getDriveModeButtonStatus() != -999999) put.addColumn(cfBytes, Bytes.toBytes("driveModeButtonStatus"), Bytes.toBytes(String.valueOf(vehicleDataObj.getDriveModeButtonStatus())));
		if (vehicleDataObj.getVCUSRSCrashOutpSts() != -999999) put.addColumn(cfBytes, Bytes.toBytes("VCUSRSCrashOutpSts"), Bytes.toBytes(String.valueOf(vehicleDataObj.getVCUSRSCrashOutpSts())));
		if (vehicleDataObj.getTextDispEna() != -999999) put.addColumn(cfBytes, Bytes.toBytes("textDispEna"), Bytes.toBytes(String.valueOf(vehicleDataObj.getTextDispEna())));
		if (vehicleDataObj.getCrsCtrlStatus() != -999999) put.addColumn(cfBytes, Bytes.toBytes("crsCtrlStatus"), Bytes.toBytes(String.valueOf(vehicleDataObj.getCrsCtrlStatus())));
		if (vehicleDataObj.getCrsTarSpd() != -999999) put.addColumn(cfBytes, Bytes.toBytes("crsTarSpd"), Bytes.toBytes(String.valueOf(vehicleDataObj.getCrsTarSpd())));
		if (vehicleDataObj.getCrsTextDisp() != -999999) put.addColumn(cfBytes, Bytes.toBytes("crsTextDisp"), Bytes.toBytes(String.valueOf(vehicleDataObj.getCrsTextDisp())));
		if (vehicleDataObj.getKeyOn() != -999999) put.addColumn(cfBytes, Bytes.toBytes("keyOn"), Bytes.toBytes(String.valueOf(vehicleDataObj.getKeyOn())));
		if (vehicleDataObj.getVehPwrlim() != -999999) put.addColumn(cfBytes, Bytes.toBytes("vehPwrlim"), Bytes.toBytes(String.valueOf(vehicleDataObj.getVehPwrlim())));
		if (!vehicleDataObj.getVehCfgInfo().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("vehCfgInfo"), Bytes.toBytes(vehicleDataObj.getVehCfgInfo()));
		if (vehicleDataObj.getVacBrkPRmu() != -999999) put.addColumn(cfBytes, Bytes.toBytes("vacBrkPRmu"), Bytes.toBytes(vehicleDataObj.getVacBrkPRmu()));
		// todo: 额外额外一个字段，表示当前数据处理时间
		put.addColumn(cfBytes, Bytes.toBytes("processTime"), Bytes.toBytes(DateUtil.getCurrentDateTime())) ;

		// 返回Put对象
		return put;
	}

	// 收尾工作，比如关闭连接
	@Override
	public void close() throws Exception {
		// f. 关闭连接
		if(null != table) table.close();
		if(null != connection) connection.close();
	}
}
