package cn.cavehicle.streaming.sink;

import cn.cavehicle.utils.DateUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * 自定义Sink，采用HiveServer2 JDBC Client 方式实时写入数据到Hive表中
 */
public class ErrorDataToHiveSink extends RichSinkFunction<String> {

	// 定义变量
	private Connection connection = null ;
	private Statement stmt = null ;

	// 准备工作，每个实例对象执行一次：创建连接等
	@Override
	public void open(Configuration parameters) throws Exception {
		// a. 加载驱动类
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		// b. 获取连接
		connection = DriverManager.getConnection(
			"jdbc:hive2://node1.itcast.cn:10000", "root", "123456"
		);
		// c. 实例化Statement对象
		stmt = connection.createStatement();
	}


	@Override
	public void invoke(String value, Context context) throws Exception {
		// d. 执行插入
		/*
			value
				|
			INSERT INTO vehicle_ods.vehicle_error_hive PARTITION (dt = '2022-04-12') VALUES ('{}')
		 */
		StringBuilder builder = new StringBuilder();
		builder.append("INSERT INTO vehicle_ods.vehicle_error_hive PARTITION (dt = '");
		builder.append(DateUtil.getCurrentDate("yyyy-MM-dd"));
		builder.append("') VALUES ('");
		builder.append(value);
		builder.append("')");
		String insertSQL = builder.toString();
		System.out.println("insert error data sql: "  + insertSQL);

		stmt.execute(insertSQL) ;
	}

	@Override
	public void close() throws Exception {
		// e. 关闭连接
		if(null != stmt) stmt.close();
		if(null != connection) connection.close();
	}

}
