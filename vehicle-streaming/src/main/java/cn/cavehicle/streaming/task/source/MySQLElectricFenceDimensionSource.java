package cn.cavehicle.streaming.task.source;

import cn.cavehicle.entity.ElectricFenceDimension;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 自定义数据源：加载MySQL数据库中【电子围栏车辆表】与【电子围栏规则设置表】关联数据，封装到Map集合
 *      key: vin
 *      value: List<ElectricFenceDimension-1, ElectricFenceDimension-2, ....>
 */
public class MySQLElectricFenceDimensionSource
		extends RichSourceFunction<Map<String, List<ElectricFenceDimension>>> {
	// 定义标识符
	private boolean isRunning = true ;

	// 定义变量
	private Connection conn = null ;
	private PreparedStatement pstmt = null ;
	// 定义变量，获取Job作业全局参数
	private ParameterTool parameterTool = null ;

	// 初始化工作，比如获取连接
	@Override
	public void open(Configuration parameters) throws Exception {
		// 实例化全局参数的值
		parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
		// a. 加载驱动类
		Class.forName(parameterTool.getRequired("jdbc.driver"));
		// b. 获取连接
		conn = DriverManager.getConnection(
			parameterTool.getRequired("jdbc.url"),
			parameterTool.getRequired("jdbc.user"),
			parameterTool.getRequired("jdbc.password")
		);
		// c. 实例化Statement对象
		pstmt = conn.prepareStatement(
			"SELECT\n" +
				"    t1.vin, t1.setting_id, t2.name, t2.address, t2.radius,\n" +
				"    t2.longitude, t2.latitude, t2.start_time, t2.end_time\n" +
				"FROM vehicle_networking.electronic_fence_vins t1\n" +
				"JOIN vehicle_networking.electronic_fence_setting t2\n" +
				"ON t1.setting_id = t2.id AND t2.status = 1"
		);
	}

	@Override
	public void run(SourceContext<Map<String, List<ElectricFenceDimension>>> ctx) throws Exception {
		while (isRunning){
			// d. 查询数据
			ResultSet resultSet = pstmt.executeQuery();
			// e. 遍历获取每条数据，封装到Map集合中
			Map<String, List<ElectricFenceDimension>> hashMap = new HashMap<>();
			while (resultSet.next()){
				// 获取车架号
				String vinValue = resultSet.getString("vin");
				// 获取每个电子围栏基本信息数，封装到实体类对象中
				ElectricFenceDimension dimension = new ElectricFenceDimension();
				dimension.setId(resultSet.getInt("setting_id"));
				dimension.setName(resultSet.getString("name"));
				dimension.setAddress(resultSet.getString("address"));
				dimension.setRadius(resultSet.getFloat("radius"));
				dimension.setLongitude(resultSet.getDouble("longitude"));
				dimension.setLatitude(resultSet.getDouble("latitude"));
				dimension.setStartTime(resultSet.getDate("start_time"));
				dimension.setEndTime(resultSet.getDate("end_time"));
				// 放入map集合中， 先判断是否已经存储vin信息
				if(hashMap.containsKey(vinValue)){
					// 获取存储电子围栏数据
					List<ElectricFenceDimension> list = hashMap.get(vinValue);
					// todo: 创建新列表，包含前面电子围栏信息数据和当前电子围栏信息数据
					List<ElectricFenceDimension> newList = new ArrayList<>(list);
					newList.add(dimension);
					// 重新设置
					hashMap.put(vinValue, newList) ;
				}else{
					hashMap.put(vinValue, Collections.singletonList(dimension)) ;
				}
			}
			// f. 将数据发送到下游
			ctx.collect(hashMap);
			// todo: 释放资源，关闭连接
			if(!resultSet.isClosed()) resultSet.close();

			// 每隔一定时间间隔，加载一次MySQL数据库中维度数据
			TimeUnit.MILLISECONDS.sleep(parameterTool.getLong("electric.vin.millionseconds", 300000));
		}
	}

	@Override
	public void cancel() {
		isRunning = false ;
	}

	// 清理工作，比如释放连接
	@Override
	public void close() throws Exception {
		if(null != pstmt) pstmt.close();
		if(null != conn) conn.close();
	}
}
