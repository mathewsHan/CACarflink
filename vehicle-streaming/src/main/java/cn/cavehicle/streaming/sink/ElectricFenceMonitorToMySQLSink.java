package cn.cavehicle.streaming.sink;

import cn.cavehicle.entity.ElectricFenceModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 自定义Sink，将分析结果数据写入到MySQL数据库表中
 */
public class ElectricFenceMonitorToMySQLSink extends RichSinkFunction<ElectricFenceModel> {

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
			"INSERT INTO vehicle_networking.electric_fence_result(\n" +
				"    vin, in_time, out_time, gps_time, lat, lng, ele_id, ele_name, address, latitude, longitude, radius\n" +
				") VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\n" +
				"ON DUPLICATE KEY UPDATE\n" +
				"out_time = VALUES(out_time), gps_time = VALUES(gps_time), lat = VALUES(lat) , lng = VALUES(lng)"
		);
	}

	@Override
	public void invoke(ElectricFenceModel model, Context context) throws Exception {
		// d. 设置属性占位符值
		pstmt.setString(1, model.getVin());
		pstmt.setString(2, model.getInTime());
		// 如果是驶入围栏，设置为null；如果是驶出围栏，设置为具体值
		String outTime = StringUtils.isEmpty(model.getOutTime()) ? null : model.getOutTime() ;
		pstmt.setString(3, outTime);
		pstmt.setObject(4, model.getGpsTime());
		pstmt.setDouble(5, model.getLat());
		pstmt.setDouble(6, model.getLng());

		pstmt.setInt(7, model.getEleId());
		pstmt.setString(8, model.getEleName());
		pstmt.setString(9, model.getAddress());
		pstmt.setDouble(10, model.getLatitude());
		pstmt.setDouble(11, model.getLongitude());
		pstmt.setFloat(12, model.getRadius());

		// e. 执行语句
		pstmt.execute();
	}

	@Override
	public void close() throws Exception {
		if(null != pstmt) pstmt.close();
		if(null != conn) conn.close();
	}

}

