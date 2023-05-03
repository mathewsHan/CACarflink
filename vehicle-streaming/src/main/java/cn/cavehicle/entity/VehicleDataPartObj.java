package cn.cavehicle.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 定义原始数据中json对象对应的部分所需字段对象
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class VehicleDataPartObj {
    //电池单体一致性差报警	0：正常 1：异常--
    private int batteryConsistencyDifferenceAlarm = -999999;
    //SOC	%--
    private int soc = -999999;
    //SOC跳变报警	0：正常 1：异常--
    private int socJumpAlarm = -999999;
    //SOC低报警	0：正常 1：异常--
    private int socLowAlarm = -999999;
    //终端时间--
    private String terminalTime = "";
    //单体电池过压报警	0：正常 1：异常--
    private int singleBatteryOverVoltageAlarm = -999999;
    //车载储能装置过压报警	0：正常 1：异常--
    private int vehicleStorageDeviceOvervoltageAlarm = -999999;
    //制动系统报警	0：正常 1：异常--
    private int brakeSystemAlarm = -999999;
    //车辆唯一编号--
    private String vin = "";
    //"0x01: 停车充电 0x02: 行车充电 0x03: 未充电  0x04:充电完成 0xFE: 异常 0xFF:无效"
    private int chargeStatus = -999999;
    //驱动电机温度报警	0：正常 1：异常--
    private int driveMotorTemperatureAlarm = -999999;
    //DC-DC状态报警	0：正常 1：异常--
    private int dcdcStatusAlarm = -999999;
    //位置时间
    private String gpsTime = "";
    //位置纬度--
    private Double lat = -999999D;
    //驱动电机控制器温度报警	0：正常 1：异常
    private int driveMotorFaultCount = -999999;
    //车速--
    private Double vehicleSpeed = -999999D;
    //位置经度--
    private Double lng = -999999D;
    //可充电储能装置电压	V--
    private Double chargeSystemVoltage = -999999D;
    //可充电储能装置电流	A--
    private Double chargeSystemCurrent = -999999D;
    //单体电池欠压报警	0：正常 1：异常--
    private int singleBatteryUnderVoltageAlarm = -999999;
    //总电压	单位：V，实际取值0.1~100V
    private Double totalVoltage = -999999D;
    //温度差异报警	0：正常 1：异常--
    private int temperatureDifferenceAlarm = -999999;
    //--电池单体电压最低值	单位：V，实际值=传输值*0.001，即实际为0~15V。0xFF,0xFE表示异常，0xFF,0xFF表示无效
    private Double minVoltageBattery = -999999D;
    //GPS是否有效（可忽略）
    private String validGps = "";
    //累计里程	单位：km--
    private Double totalOdometer = -999999D;
    //车速（可忽略）	单位：km/h
    private Double speed = -999999D;
    //SOC过高报警	0：正常 1：异常--
    private int socHighAlarm = -999999;
    //车载储能装置欠压报警	0：正常 1：异常--
    private int vehicleStorageDeviceUndervoltageAlarm = -999999;
    //总电流	单位：A，实际值 = 传输值 * 0.1-1000，即实际取值为-1000~1000A
    private Double totalCurrent = -999999D;
    //电池高温报警	0：正常 1：异常--
    private int batteryAlarm = -999999;
    //可充电储能系统不匹配报警	0：正常 1：异常--
    private int rechargeableStorageDeviceMismatchAlarm = -999999;
    //车载储能装置类型过充报警	0：正常 1：异常--
    private int vehiclePureDeviceTypeOvercharge = -999999;
    //--电池单体电压最高值	单位：V，实际值=传输值*0.001，即实际为0~15V。0xFF,0xFE表示异常，0xFF,0xFF表示无效
    private Double maxVoltageBattery = -999999D;
    //DC-DC温度报警	0：正常 1：异常--
    private int dcdcTemperatureAlarm = -999999;
    //同validGps（可忽略）
    private String isValidGps = "";
    //驱动电机控制器温度报警	0：正常 1：异常
    private int driveMotorControllerTemperatureAlarm = -999999;
    //是否补发	TRUE:补发数据 ； FALSE:实时数据
    private String repay = "";
    //车辆状态	0x01: 车辆启动状态，0x02：熄火状态 0x03：其他状态，0xFE：异常，0xFF：无效
    private int carStatus = -999999;
    //前电机故障代码 json数据中，ecuErrCodeDataList数组中ecuType=4--
    private String IpuFaultCodes = "";
    //能量回收状态	高/低
    private int energyRecoveryStatus = -999999;
    //点火状态	0：未点火 ；2：已点火
    private int fireStatus = -999999;
    //运行模式	0x01: 纯电 0x02 混动 0x03 燃油 0xFE: 异常 0xFF: 无效
    private int carMode = -999999;
    //高压互锁状态报警	0：正常 1：异常--
    private int highVoltageInterlockStateAlarm = -999999;
    //绝缘报警	0：正常 1：异常--
    private int insulationAlarm = -999999;
    //续航里程信息	单位：km--
    private int mileageInformation = -999999;
    //当前电量	%
    private Double remainPower = -999999D;
    //发动机状态	0：stop  1：crank   2：running
    private String engineStatus = "";
    //ABS故障 "0x0:No error 0x1:Error"--
    private String absStatus = "";
    //EPB故障状态 "0x0:no error 0x1:not defined 0x2:not defined 0x3:error"--
    private String VcuBrkErr = "";
    //ESP故障 "0x0:No error 0x1:Error"--
    private String ESP_TCSFailStatus = "";
    //助力转向故障 "0x0:No Failed 0x1:Failed"--
    private String EPS_EPSFailed = "";
    //电池故障代码 json数据中，ecuErrCodeDataList数组中ecuType=2--
    private String BcuFaultCodes = "";
    //VCU故障代码 json数据中，ecuErrCodeDataList数组中ecuType=1--
    private String VcuFaultCode = "";
    /**
     * --充电机故障码  "0x0:当前无故障 0x1:12V电池电压过高 0x2:12V电池电压过低 0x3:CP内部6V电压异常 0x4:CP内部9V电压异常
     * 0x5:CP内部频率异常 0x6:CP内部占空比异常 0x7:CAN收发器异常 0x8:内部SCI通信失败 0x9:内部SCICRC错误 0xA:输出过压关机
     * 0xB:输出低压关机 0xC:交流输入低压关机 0xD:输入过压关机 0xE:环境温度过低关机 0xF:环境温度过高关机
     * 0x10:充电机PFC电压欠压 0x11:输入过载 0x12:输出过载 0x13:自检故障 0x14:外部CANbusoff 0x15:内部CANbusoff
     * 0x16:外部CAN通信超时 0x17:外部CAN使能超时 0x18:外部CAN通信错误 0x19:输出短路 0x1A:充电参数错误
     * 0x1B:充电机PFC电压过压 0x1C:内部SCI通信失败 0x1D:过功率 0x1E:PFC电感过温 0x1F:LLC变压器过温 0x20:M1功率板过温
     * 0x21:PFC温度降额 0x22:LLC温度降额 0x23:M1板温度降额 0x24:Air环境温度降额"
     * json数据中，ecuErrCodeDataList数组中ecuType=5
     */
    private String ObcFaultCode  = "";
    //最高温度值	单位：℃。实际值：-40~210。0xFE表示异常，1xFF表示无效
    private Double maxTemperatureValue = -999999D;
    //最低温度值	单位：℃。实际值：-40~210。0xFE表示异常，1xFF表示无效
    private Double minTemperatureValue = -999999D;
    //DCDC故障码 json数据中，ecuErrCodeDataList数组中ecuType=3
    private String DcdcFaultCode = "";
    //电池极注高温报警 "0x0:Noerror 0x1:error"
    private String battPoleTOver = "";
    //AC系统故障 "0x0:NotFailure 0x1:Failure"
    private int acSystemFailure = -999999;
    //气囊系统报警灯状态  "0x0:Lamp off-no failure 0x1:Lamp on-no failure 0x2:Lamp flashing-no failure 0x3:Failure-failure present"
    private int airBagWarningLamp = -999999;
    //电池充电状态 "0x0:uncharged 0x1:charging 0x2:fullofcharge 0x3:chargeend"
    private int batteryChargeStatus = -999999;
    //充电枪连接状态	0:解锁1:锁定2:失败
    private int chargingGunConnectionState = -999999;
    //单体电池电压列表	Array类型的数组格式
    private String batteryVoltage = "" ;
    //电池模块温度列表
    private String probeTemperatures = "";

    // 扩展字段 终端时间
    private Long terminalTimeStamp = -999999L;
    //扩展字段，用于存储异常数据
    private String errorData = "";

    // ===================================================
    //县或区
    private String county = "";
    //省份
    private String province = "";
    //城市
    private String city = "";
    //详细地址
    private String address = "";
    // ===================================================

    // 重写ItcastDataPartObj对象的toString方法
    @Override
    public String toString() {
        return "ItcastDataPartObj{" +
                "batteryConsistencyDifferenceAlarm=" + batteryConsistencyDifferenceAlarm +
                ", soc=" + soc +
                ", socJumpAlarm=" + socJumpAlarm +
                ", socLowAlarm=" + socLowAlarm +
                ", terminalTime='" + terminalTime + '\'' +
                ", singleBatteryOverVoltageAlarm=" + singleBatteryOverVoltageAlarm +
                ", vehicleStorageDeviceOvervoltageAlarm=" + vehicleStorageDeviceOvervoltageAlarm +
                ", brakeSystemAlarm=" + brakeSystemAlarm +
                ", vin='" + vin + '\'' +
                ", chargeStatus=" + chargeStatus +
                ", driveMotorTemperatureAlarm=" + driveMotorTemperatureAlarm +
                ", dcdcStatusAlarm=" + dcdcStatusAlarm +
                ", gpsTime='" + gpsTime + '\'' +
                ", lat=" + lat +
                ", driveMotorFaultCount=" + driveMotorFaultCount +
                ", vehicleSpeed=" + vehicleSpeed +
                ", lng=" + lng +
                ", chargeSystemVoltage=" + chargeSystemVoltage +
                ", chargeSystemCurrent=" + chargeSystemCurrent +
                ", singleBatteryUnderVoltageAlarm=" + singleBatteryUnderVoltageAlarm +
                ", totalVoltage=" + totalVoltage +
                ", temperatureDifferenceAlarm=" + temperatureDifferenceAlarm +
                ", minVoltageBattery=" + minVoltageBattery +
                ", validGps='" + validGps + '\'' +
                ", totalOdometer=" + totalOdometer +
                ", speed=" + speed +
                ", socHighAlarm=" + socHighAlarm +
                ", vehicleStorageDeviceUndervoltageAlarm=" + vehicleStorageDeviceUndervoltageAlarm +
                ", totalCurrent=" + totalCurrent +
                ", batteryAlarm=" + batteryAlarm +
                ", rechargeableStorageDeviceMismatchAlarm=" + rechargeableStorageDeviceMismatchAlarm +
                ", vehiclePureDeviceTypeOvercharge=" + vehiclePureDeviceTypeOvercharge +
                ", maxVoltageBattery=" + maxVoltageBattery +
                ", dcdcTemperatureAlarm=" + dcdcTemperatureAlarm +
                ", isValidGps='" + isValidGps + '\'' +
                ", driveMotorControllerTemperatureAlarm=" + driveMotorControllerTemperatureAlarm +
                ", repay='" + repay + '\'' +
                ", carStatus=" + carStatus +
                ", IpuFaultCodes='" + IpuFaultCodes + '\'' +
                ", energyRecoveryStatus=" + energyRecoveryStatus +
                ", fireStatus=" + fireStatus +
                ", carMode=" + carMode +
                ", highVoltageInterlockStateAlarm=" + highVoltageInterlockStateAlarm +
                ", insulationAlarm=" + insulationAlarm +
                ", mileageInformation=" + mileageInformation +
                ", remainPower=" + remainPower +
                ", engineStatus='" + engineStatus + '\'' +
                ", absStatus='" + absStatus + '\'' +
                ", VcuBrkErr='" + VcuBrkErr + '\'' +
                ", ESP_TCSFailStatus='" + ESP_TCSFailStatus + '\'' +
                ", EPS_EPSFailed='" + EPS_EPSFailed + '\'' +
                ", BcuFaultCodes='" + BcuFaultCodes + '\'' +
                ", VcuFaultCode='" + VcuFaultCode + '\'' +
                ", ObcFaultCode='" + ObcFaultCode + '\'' +
                ", maxTemperatureValue=" + maxTemperatureValue +
                ", minTemperatureValue=" + minTemperatureValue +
                ", DcdcFaultCode='" + DcdcFaultCode + '\'' +
                ", battPoleTOver='" + battPoleTOver + '\'' +
                ", acSystemFailure=" + acSystemFailure +
                ", airBagWarningLamp=" + airBagWarningLamp +
                ", batteryChargeStatus=" + batteryChargeStatus +
                ", chargingGunConnectionState=" + chargingGunConnectionState +
                ", batteryVoltage='" + batteryVoltage + '\'' +
                ", probeTemperatures='" + probeTemperatures + '\'' +
                ", terminalTimeStamp=" + terminalTimeStamp +
                ", errorData='" + errorData + '\'' +
                ", country='" + county + '\'' +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", address='" + address + '\'' +
                '}';
    }

}	