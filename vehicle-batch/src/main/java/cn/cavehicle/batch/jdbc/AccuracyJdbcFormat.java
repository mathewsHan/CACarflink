package cn.cavehicle.batch.jdbc;

import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * 由于使用Flink JDBC方式InputFormat和OutputFormat读取写入数据，需要传递参数信息，代码冗余，进行抽象封装
 */
public class AccuracyJdbcFormat {

	// 加载属性配置文件
	private static ParameterTool parameterTool ;
	static {
		try{
			parameterTool = ParameterTool.fromPropertiesFile(
				AccuracyJdbcFormat.class.getClassLoader().getResourceAsStream("config.properties")
			);
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	/**
	 * 构建JDBCInputFormat对象，从Hive表中加载数据
	 */
	public static JDBCInputFormat getHiveJDBCInputFormat(String querySql, RowTypeInfo rowTypeInfo) {
		// a. 数据库属性字段值
		String driverName = parameterTool.getRequired("hive.jdbc.driver") ;
		String url = parameterTool.getRequired("hive.jdbc.url") ;

		// b. 设置参数值，构建对象
		JDBCInputFormat inputFormat = JDBCInputFormat
			.buildJDBCInputFormat()
			.setDrivername(driverName)
			.setDBUrl(url)
			.setQuery(querySql)
			.setRowTypeInfo(rowTypeInfo)
			.finish();

		// c. 返回实例
		return inputFormat ;
	}

	/**
	 * 构建JDBCOutputFormat对象，向MySQL数据库写入数据
	 */
	public static JDBCOutputFormat getMySQLJDBCOutputFormat(String insertSql, int[] typesArray){
		// a. 数据库属性字段
		String driverName = parameterTool.getRequired("mysql.jdbc.driver");
		String url = parameterTool.getRequired("mysql.jdbc.url");
		String userName = parameterTool.getRequired("mysql.jdbc.user");
		String password = parameterTool.getRequired("mysql.jdbc.password") ;

		// b. 设置参数，创建对象
		JDBCOutputFormat outputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
			.setDrivername(driverName)
			.setDBUrl(url)
			.setUsername(userName)
			.setPassword(password)
			.setQuery(insertSql)
			.setSqlTypes(typesArray)
			.finish();

		// c. 返回实例
		return outputFormat ;
	}

}
