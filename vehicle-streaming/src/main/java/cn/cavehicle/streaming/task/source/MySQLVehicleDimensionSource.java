package cn.cavehicle.streaming.task.source;

import cn.cavehicle.entity.VehicleDimension;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 自定义数据源Source：从MySQL数据库加载车辆基本信息数据，多个表关联查询结果。
 */
public class MySQLVehicleDimensionSource extends RichSourceFunction<Map<String, VehicleDimension>> {
	// 定义标识符，表示是否产生数据
	private boolean isRunning = true ;

	// 定义变量
	private Connection conn = null ;
	private PreparedStatement pstmt = null ;
	// 全局参数
	private ParameterTool parameterTool = null ;

	@Override
	public void open(Configuration parameters) throws Exception {
		parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
		// a. 加载驱动类
		Class.forName(parameterTool.getRequired("jdbc.driver")) ;
		// b. 获取连接
		conn = DriverManager.getConnection(
			parameterTool.getRequired("jdbc.url"), parameterTool.getRequired("jdbc.user"), parameterTool.getRequired("jdbc.password")
		);
		// c.实例化Statement对象
		pstmt = conn.prepareStatement(
			"\n" +
				"SELECT\n" +
				"       t12.vin, t12.model_code, t12.series_name, t12.series_code, t12.vehicle_id,\n" +
				"       t12.model_name, t12.nick_name, t3.sales_date, t4.car_type\n" +
				"FROM\n" +
				"(\n" +
				"    SELECT\n" +
				"        t1.vin, t1.model_code, t1.series_name, t1.series_code, t1.vehicle_id,\n" +
				"        t2.show_name AS model_name, t2.nick_name\n" +
				"    FROM vehicle_networking.dcs_vehicles t1\n" +
				"    LEFT JOIN vehicle_networking.t_car_type_code t2\n" +
				"    ON t1.model_code = t2.model_code\n" +
				") t12\n" +
				"LEFT JOIN (\n" +
				"    SELECT vehicle_id, MAX(sales_date) AS sales_date\n" +
				"    FROM vehicle_networking.dcs_sales GROUP BY vehicle_id\n" +
				") t3 ON t12.vehicle_id = t3.vehicle_id\n" +
				"LEFT JOIN (\n" +
				"    SELECT tn.vin, '网约车' AS car_type FROM vehicle_networking.t_net_car tn\n" +
				"    union all\n" +
				"    SELECT tt.vin, '出租车' AS car_type FROM vehicle_networking.t_taxi tt\n" +
				"    union all\n" +
				"    SELECT tc.vin, '私家车' AS car_type FROM vehicle_networking.t_private_car tc\n" +
				"    union all\n" +
				"    SELECT tm.vin, 'Model车' AS car_type FROM vehicle_networking.t_model_car tm\n" +
				") t4 ON t12.vin = t4.vin"
		);
	}

	@Override
	public void run(SourceContext<Map<String, VehicleDimension>> ctx) throws Exception {
		while (isRunning){
			// d. 查询数据库
			ResultSet resultSet = pstmt.executeQuery();
			// e. 获取数据，添加到Map集合中
			Map<String, VehicleDimension> hashMap = new HashMap<>();
			while (resultSet.next()){
				// 获取vin车架号
				String vin = resultSet.getString("vin");
				// 获取销售日期
				String salesDate = resultSet.getString("sales_date");
				String liveTime = "-1" ;
				if(StringUtils.isNotEmpty(salesDate)){
					// 使用年限 = 当前日期 - 销售日期
					liveTime = String.valueOf(
						(new Date().getTime() - resultSet.getDate("sales_date").getTime()) / 1000 / 3600 / 24 / 365
					);
				}
				// 创建实体类对象
				VehicleDimension vehicleDimension = new VehicleDimension(
					vin, resultSet.getString("model_code"), resultSet.getString("model_name"),
					resultSet.getString("series_code"), resultSet.getString("series_name"),
					salesDate, resultSet.getString("car_type"),
					resultSet.getString("nick_name"), liveTime
				);
				// 添加到map集合
				hashMap.put(vin, vehicleDimension);
			}
			// f. 输出获取数据
			ctx.collect(hashMap);
			// g. 释放资源
			if(!resultSet.isClosed()) resultSet.close();

			// h. 每个多久加载车辆维度数据
			TimeUnit.MILLISECONDS.sleep(parameterTool.getLong("vehinfo.millionseconds", 18000000L));
		}
	}

	@Override
	public void cancel() {
		isRunning = false ;
	}

	@Override
	public void close() throws Exception {
		if(null != pstmt) pstmt.close();
		if(null != conn) conn.close();
	}
}