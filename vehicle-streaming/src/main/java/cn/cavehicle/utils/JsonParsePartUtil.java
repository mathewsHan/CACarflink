package cn.cavehicle.utils;

import cn.cavehicle.entity.VehicleDataPartObj;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

/**
 * JSON 字符串解析工具类，解析部分核心重要字段数据
 */
public class JsonParsePartUtil {

    /**
     * 解析json成为VehicleDataPartObj对象
     */
    public static VehicleDataPartObj parseJsonToObject(String jsonString) {
        VehicleDataPartObj vehiclePartObj = new VehicleDataPartObj();
        try {
            Map<String, Object> vehicleMap = jsonToMap(jsonString);
            vehiclePartObj.setBatteryConsistencyDifferenceAlarm(convertIntType("batteryConsistencyDifferenceAlarm", vehicleMap));
            vehiclePartObj.setSoc(convertIntType("soc", vehicleMap));
            vehiclePartObj.setSocJumpAlarm(convertIntType("socJumpAlarm", vehicleMap));
            vehiclePartObj.setSocLowAlarm(convertIntType("socLowAlarm", vehicleMap));
            vehiclePartObj.setTerminalTime(convertStringType("terminalTime", vehicleMap));
            vehiclePartObj.setSingleBatteryOverVoltageAlarm(convertIntType("singleBatteryOverVoltageAlarm", vehicleMap));
            vehiclePartObj.setVehicleStorageDeviceOvervoltageAlarm(convertIntType("vehicleStorageDeviceOvervoltageAlarm", vehicleMap));
            vehiclePartObj.setBrakeSystemAlarm(convertIntType("brakeSystemAlarm", vehicleMap));
            vehiclePartObj.setVin(convertStringType("vin", vehicleMap).toUpperCase());
            vehiclePartObj.setChargeStatus(convertIntType("chargeStatus", vehicleMap));
            vehiclePartObj.setDriveMotorTemperatureAlarm(convertIntType("driveMotorTemperatureAlarm", vehicleMap));
            vehiclePartObj.setDcdcStatusAlarm(convertIntType("dcdcStatusAlarm", vehicleMap));
            vehiclePartObj.setLat(convertDoubleType("lat", vehicleMap));
            vehiclePartObj.setVehicleSpeed(convertDoubleType("vehicleSpeed", vehicleMap));
            vehiclePartObj.setLng(Double.parseDouble(convertStringType("lng", vehicleMap)));
            vehiclePartObj.setGpsTime(convertStringType("gpsTime", vehicleMap));
            vehiclePartObj.setSingleBatteryUnderVoltageAlarm(convertIntType("singleBatteryUnderVoltageAlarm", vehicleMap));
            vehiclePartObj.setTotalVoltage(convertDoubleType("totalVoltage", vehicleMap));
            vehiclePartObj.setTemperatureDifferenceAlarm(convertIntType("temperatureDifferenceAlarm", vehicleMap));
            vehiclePartObj.setMinVoltageBattery(convertDoubleType("minVoltageBattery", vehicleMap));
            vehiclePartObj.setValidGps(convertStringType("validGps", vehicleMap));
            vehiclePartObj.setTotalOdometer(convertDoubleType("totalOdometer", vehicleMap));
            vehiclePartObj.setSpeed(convertDoubleType("speed", vehicleMap));
            vehiclePartObj.setSocHighAlarm(convertIntType("socHighAlarm", vehicleMap));
            vehiclePartObj.setVehicleStorageDeviceUndervoltageAlarm(convertIntType("vehicleStorageDeviceUndervoltageAlarm", vehicleMap));
            vehiclePartObj.setTotalCurrent(convertDoubleType("totalCurrent", vehicleMap));
            vehiclePartObj.setBatteryAlarm(convertIntType("batteryAlarm", vehicleMap));
            vehiclePartObj.setBattPoleTOver(convertStringType("battPoleTOver", vehicleMap));
            vehiclePartObj.setAcSystemFailure(convertIntType("acSystemFailure", vehicleMap));
            vehiclePartObj.setAirBagWarningLamp(convertIntType("airBagWarningLamp", vehicleMap));
            vehiclePartObj.setBatteryChargeStatus(convertIntType("batteryChargeStatus", vehicleMap));
            vehiclePartObj.setChargingGunConnectionState(convertIntType("chargingGunConnectionState", vehicleMap));
            vehiclePartObj.setRechargeableStorageDeviceMismatchAlarm(convertIntType("rechargeableStorageDeviceMismatchAlarm", vehicleMap));
            vehiclePartObj.setVehiclePureDeviceTypeOvercharge(convertIntType("vehiclePureDeviceTypeOvercharge", vehicleMap));
            vehiclePartObj.setMaxVoltageBattery(convertDoubleType("maxVoltageBattery", vehicleMap));
            vehiclePartObj.setDcdcTemperatureAlarm(convertIntType("dcdcTemperatureAlarm", vehicleMap));
            vehiclePartObj.setIsValidGps(convertStringType("isValidGps", vehicleMap));
            vehiclePartObj.setDriveMotorControllerTemperatureAlarm(convertIntType("driveMotorControllerTemperatureAlarm", vehicleMap));
            vehiclePartObj.setRepay(convertStringType("repay", vehicleMap));
            vehiclePartObj.setEnergyRecoveryStatus(convertIntType("energyRecoveryStatus", vehicleMap));
            vehiclePartObj.setFireStatus(convertIntType("fireStatus", vehicleMap));
            vehiclePartObj.setCarMode(convertIntType("carMode", vehicleMap));
            vehiclePartObj.setHighVoltageInterlockStateAlarm(convertIntType("highVoltageInterlockStateAlarm", vehicleMap));
            vehiclePartObj.setInsulationAlarm(convertIntType("insulationAlarm", vehicleMap));
            vehiclePartObj.setMileageInformation(convertIntType("mileageInformation", vehicleMap));
            vehiclePartObj.setRemainPower(convertDoubleType("remainPower", vehicleMap));
            vehiclePartObj.setEngineStatus(convertStringType("engineStatus", vehicleMap));
            vehiclePartObj.setAbsStatus(convertStringType("absStatus", vehicleMap));
            vehiclePartObj.setVcuBrkErr(convertStringType("VcuBrkErr", vehicleMap));
            vehiclePartObj.setESP_TCSFailStatus(convertStringType("ESP_TCSFailStatus", vehicleMap));
            vehiclePartObj.setEPS_EPSFailed(convertStringType("EPS_EPSFailed", vehicleMap));
            vehiclePartObj.setMaxTemperatureValue(convertDoubleType("maxTemperatureValue", vehicleMap));
            vehiclePartObj.setMinTemperatureValue(convertDoubleType("minTemperatureValue", vehicleMap));

            /* ------------------------------------------nevChargeSystemVoltageDtoList 可充电储能子系统电压信息列表-------------------------------------- */
            List<Map<String, Object>> nevChargeSystemVoltageDtoList = jsonToList(
                vehicleMap.getOrDefault("nevChargeSystemVoltageDtoList", "[]").toString()
            );
            if (!nevChargeSystemVoltageDtoList.isEmpty()) {
                // 只取list中的第一个map集合
                Map<String, Object> nevChargeSystemVoltageDtoMap = nevChargeSystemVoltageDtoList.get(0);
                vehiclePartObj.setBatteryVoltage(convertStringType("batteryVoltage", nevChargeSystemVoltageDtoMap));
                vehiclePartObj.setChargeSystemVoltage(convertDoubleType("chargeSystemVoltage",nevChargeSystemVoltageDtoMap));
                vehiclePartObj.setChargeSystemCurrent(convertDoubleType("chargeSystemCurrent", nevChargeSystemVoltageDtoMap));
            }
            /* -------------------------------------------------------------------------------------------------------------- */

            /* -----------------------------------------nevChargeSystemTemperatureDtoList------------------------------------ */
            List<Map<String, Object>> nevChargeSystemTemperatureDtoList = jsonToList(
                vehicleMap.getOrDefault("nevChargeSystemTemperatureDtoList", "[]").toString()
            );
            if (!nevChargeSystemTemperatureDtoList.isEmpty()) {
                Map<String, Object> nevChargeSystemTemperatureMap = nevChargeSystemTemperatureDtoList.get(0);
                vehiclePartObj.setProbeTemperatures(convertStringType("probeTemperatures", nevChargeSystemTemperatureMap));
            }
            /* -------------------------------------------------------------------------------------------------------------- */

            /* --------------------------------------------ecuErrCodeDataList------------------------------------------------ */
            Map<String, Object> xcuerrinfoMap = jsonToMap(
                vehicleMap.getOrDefault("xcuerrinfo", "{}").toString()
            );
            if (!xcuerrinfoMap.isEmpty()) {
                List<Map<String, Object>> ecuErrCodeDataList = jsonToList(
                    xcuerrinfoMap.getOrDefault("ecuErrCodeDataList","[]").toString()
                ) ;
                if (ecuErrCodeDataList.size() > 4) {
                    vehiclePartObj.setVcuFaultCode(convertStringType("errCodes", ecuErrCodeDataList.get(0)));
                    vehiclePartObj.setBcuFaultCodes(convertStringType("errCodes", ecuErrCodeDataList.get(1)));
                    vehiclePartObj.setDcdcFaultCode(convertStringType("errCodes", ecuErrCodeDataList.get(2)));
                    vehiclePartObj.setIpuFaultCodes(convertStringType("errCodes", ecuErrCodeDataList.get(3)));
                    vehiclePartObj.setObcFaultCode(convertStringType("errCodes", ecuErrCodeDataList.get(4)));
                }
            }
            /* -------------------------------------------------------------------------------------------------------------- */

            // carStatus不在有效范围，设置值为255
            if (convertStringType("carStatus", vehicleMap).length() > 3) {
                vehiclePartObj.setCarStatus(255);
            } else {
                vehiclePartObj.setCarStatus(convertIntType("carStatus", vehicleMap));
            }
            // terminalTime字段不为空，设置标记时间为terminalTime时间
            if(!vehiclePartObj.getTerminalTime().isEmpty()){
                vehiclePartObj.setTerminalTimeStamp(
                    DateUtil.convertStringToDate(vehiclePartObj.getTerminalTime(), DateUtil.DATE_TIME_FORMAT).getTime()
                );
                // 终端时间延迟五个小时，设置为默认值
                if(vehiclePartObj.getTerminalTimeStamp() > (System.currentTimeMillis() + 1000 * 60 * 5)){
                    vehiclePartObj.setTerminalTimeStamp(-999999L);
                }
            }
        } catch (Exception e){
            vehiclePartObj.setErrorData(jsonString);
            System.out.println("json 数据格式错误...");
        }
        // 如果没有VIN号和终端时间，则为无效数据
        if(vehiclePartObj.getVin().isEmpty() || vehiclePartObj.getTerminalTime().isEmpty() || vehiclePartObj.getTerminalTimeStamp() < 1){
            if(vehiclePartObj.getVin().isEmpty()){
                System.out.println("vin.isEmpty");
            }
            if(vehiclePartObj.getTerminalTime().isEmpty()){
                System.out.println("terminalTime.isEmpty");
            }
            vehiclePartObj.setErrorData(jsonString);
        }
        return vehiclePartObj;
    }

    /**
     * 将Json对象转换成Map
     */
    private static Map<String, Object> jsonToMap(String jsonString) throws JSONException {
        JSONObject jsonObject = new JSONObject(jsonString);
        Map<String, Object> result = new HashMap<>();
        Iterator<String> iterator = jsonObject.keys();
        while (iterator.hasNext()) {
            String key = iterator.next();
            Object value = jsonObject.get(key);
            result.put(key, value);
        }
        return result;
    }

    /**
     * 将数组转换为List，数组内部的json字符串转换为map
     */
    private static List<Map<String, Object>> jsonToList(String jsonString) throws JSONException {
        List<Map<String, Object>> resultList = new ArrayList<>();
        JSONArray jsonArray = new JSONArray(jsonString);
        for (int i = 0; i < jsonArray.length(); i++) {
            Map<String, Object> map =jsonToMap(jsonArray.get(i).toString());
            resultList.add(map);
        }
        return resultList;
    }

    /**
     * 提取类型重复转换代码
     */
    private static int convertIntType(String fieldName, Map<String, Object> map) {
        return Integer.parseInt(map.getOrDefault(fieldName, -999999).toString());
    }
    private static String convertStringType(String fieldName, Map<String, Object> map) {
        return map.getOrDefault(fieldName, "").toString();
    }
    private static double convertDoubleType(String fieldName, Map<String, Object> map) {
        return Double.parseDouble(map.getOrDefault(fieldName, -999999D).toString());
    }

}