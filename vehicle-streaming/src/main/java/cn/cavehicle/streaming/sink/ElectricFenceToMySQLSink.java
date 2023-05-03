package cn.cavehicle.streaming.sink;

import cn.cavehicle.entity.ElectricFenceModel;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 自定义Sink，将分析结果数据写入到MySQL数据库表中
 */
public class ElectricFenceToMySQLSink extends RichSinkFunction<ElectricFenceModel> {

	// 定义变量
	private Connection conn = null ;
	private PreparedStatement pstmt = null ;
	// 定义变量，获取Job作业全局参数
	private ParameterTool parameterTool = null ;

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
	}

	@Override
	public void invoke(ElectricFenceModel model, Context context) throws Exception {
		// todo 1: 获取属性值，决定如何保存数据到MySQL数据库表中
		int fenceStatus = model.getFenceStatus();
		/*
							fenceStatus
			插入insert            1           驶入电子围栏

			更新update            0           输出电子围栏
		 */
		// todo 2: 插入数据，驶入电子围栏
		if(1 == fenceStatus){
			// c. 实例化Statement对象
			pstmt = conn.prepareStatement(
				"INSERT INTO vehicle_networking.electric_fence( " +
					"    vin, in_time, gps_time, lat, lng, " +
					"ele_id, ele_name, address, latitude, longitude, radius " +
					") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
			);
			// d. 设置占位符值
			pstmt.setString(1, model.getVin());
			pstmt.setString(2, model.getInTime());
			pstmt.setObject(3, model.getGpsTime());
			pstmt.setDouble(4, model.getLat());
			pstmt.setDouble(5, model.getLng());

			pstmt.setInt(6, model.getEleId());
			pstmt.setString(7, model.getEleName());
			pstmt.setString(8, model.getAddress());
			pstmt.setDouble(9, model.getLatitude());
			pstmt.setDouble(10, model.getLongitude());
			pstmt.setFloat(11, model.getRadius());

			// e. 执行语句
			pstmt.execute();
		}
		// todo 3: 驶出电子围栏
		else if(0 == fenceStatus){
			// c. 实例化Statement对象
			pstmt = conn.prepareStatement(
				"UPDATE vehicle_networking.electric_fence " +
					"SET out_time = ?, gps_time = ?, lat = ?, lng = ? " +
					"wHERE id = ?"
			);
			// d. 设置占位符值
			pstmt.setString(1, model.getOutTime());
			pstmt.setObject(2, model.getGpsTime());
			pstmt.setDouble(3, model.getLat());
			pstmt.setDouble(4, model.getLng());
			pstmt.setLong(5, model.getId());
			// e. 执行更新啊哦做
			pstmt.executeUpdate();
		}
	}

	@Override
	public void close() throws Exception {
		if(null != pstmt) pstmt.close();
		if(null != conn) conn.close();
	}
}
