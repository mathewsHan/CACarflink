package cn.cavehicle.streaming.sink;

import cn.cavehicle.entity.OnlineDataModel;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 自定义Sink数据接收器，将远程诊断实时故障统计结果数据保存到MySQL数据库表中
 */
public class OnlineDataToMySQLSink extends RichSinkFunction<OnlineDataModel> {
	// 定义变量，数据库操作时对象声明
	private Connection conn = null ;
	private PreparedStatement pstmt = null ;

	@Override
	public void open(Configuration parameters) throws Exception {
		// 定义变量，存储应用运行时全局参数
		ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

		// a. 加载驱动
		Class.forName(parameterTool.getRequired("jdbc.driver")) ;
		// b. 获取连接
		conn = DriverManager.getConnection(
			parameterTool.getRequired("jdbc.url"), parameterTool.getRequired("jdbc.user"), parameterTool.getRequired("jdbc.password")
		);
		// c. 实例化Statement对象
		pstmt = conn.prepareStatement(
			"INSERT INTO vehicle_networking.online_data_result (\n" +
				"    vin, is_alarm, alarm_name, earliest_time, charge_flag,\n" +
				"    terminal_time, speed, soc, lat, lng, mileage, max_voltage_battery,\n" +
				"    min_voltage_battery, max_temperature_value, min_temperature_value,\n" +
				"    total_voltage, total_current, battery_voltage, probe_temperatures,\n" +
				"    series_name, model_name, live_time, sales_date, car_type,\n" +
				"    province, city, county, address\n" +
				") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\n" +
				"ON DUPLICATE KEY UPDATE\n" +
				"    is_alarm = VALUES(is_alarm), alarm_name = VALUES(alarm_name), charge_flag = VALUES(charge_flag),\n" +
				"    terminal_time = VALUES(terminal_time), speed = VALUES(speed), soc = VALUES(soc),\n" +
				"    lat = VALUES(lat), lng = VALUES(lng), mileage = VALUES(mileage),\n" +
				"    max_voltage_battery = VALUES(max_voltage_battery),min_voltage_battery = VALUES(min_voltage_battery),\n" +
				"    max_temperature_value = VALUES(max_temperature_value), min_temperature_value = VALUES(min_temperature_value),\n" +
				"    total_voltage = VALUES(total_voltage), total_current = VALUES(total_current),\n" +
				"    battery_voltage = VALUES(battery_voltage), probe_temperatures = VALUES(probe_temperatures),\n" +
				"    series_name = VALUES(series_name), model_name = VALUES(model_name), live_time = VALUES(live_time),\n" +
				"    sales_date = VALUES(sales_date), car_type = VALUES(car_type), province = VALUES(province),\n" +
				"    city = VALUES(city), county = VALUES(county), address = VALUES(address)"
		);
	}

	@Override
	public void invoke(OnlineDataModel onlineDataModel, Context context) throws Exception {
		// d. 设置属性占位符值
		pstmt.setString(1, onlineDataModel.getVin());
		pstmt.setInt(2, onlineDataModel.getIsAlarm());
		pstmt.setString(3, onlineDataModel.getAlarmName());
		pstmt.setString(4, onlineDataModel.getEarliestTime());
		pstmt.setInt(5, onlineDataModel.getChargeFlag());
		pstmt.setString(6, onlineDataModel.getTerminalTime());
		pstmt.setDouble(7, onlineDataModel.getSpeed());
		pstmt.setInt(8, onlineDataModel.getSoc());
		pstmt.setDouble(9, onlineDataModel.getLat());
		pstmt.setDouble(10, onlineDataModel.getLng());
		pstmt.setDouble(11, onlineDataModel.getMileage());
		pstmt.setDouble(12, onlineDataModel.getMaxVoltageBattery());
		pstmt.setDouble(13, onlineDataModel.getMinVoltageBattery());
		pstmt.setDouble(14, onlineDataModel.getMaxTemperatureValue());
		pstmt.setDouble(15, onlineDataModel.getMinTemperatureValue());
		pstmt.setDouble(16, onlineDataModel.getTotalVoltage());
		pstmt.setDouble(17, onlineDataModel.getTotalCurrent());
		pstmt.setString(18, onlineDataModel.getBatteryVoltage());
		pstmt.setString(19, onlineDataModel.getProbeTemperatures());
		pstmt.setString(20, onlineDataModel.getSeriesName());
		pstmt.setString(21, onlineDataModel.getModelName());
		pstmt.setString(22, onlineDataModel.getLiveTime());
		pstmt.setString(23, onlineDataModel.getSalesDate());
		pstmt.setString(24, onlineDataModel.getCarType());
		pstmt.setString(25, onlineDataModel.getProvince());
		pstmt.setString(26, onlineDataModel.getCity());
		pstmt.setString(27, onlineDataModel.getCounty());
		pstmt.setString(28, onlineDataModel.getAddress());

		// e. 执行语句
		pstmt.execute();
	}

	@Override
	public void close() throws Exception {
		if(null != pstmt) pstmt.close();
		if(null != conn) conn.close();
	}
}
