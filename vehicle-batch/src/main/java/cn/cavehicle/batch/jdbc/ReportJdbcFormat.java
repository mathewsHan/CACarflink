package cn.cavehicle.batch.jdbc;

import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * 驾驶行程分析模块离线数据导出：从Phoenix视图中查询数据，到出到MySQL数据库表汇总
 */
public class ReportJdbcFormat {

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
	 * 构建JDBCInputFormat对象，从Phoenix视图中查询数据
	 */
	public static JDBCInputFormat getPhoenixJDBCInputFormat(String querySql, RowTypeInfo rowTypeInfo) {
		// a. 数据库属性字段值
		String driverName = parameterTool.getRequired("phoenix.driver") ;
		String url = parameterTool.getRequired("phoenix.url") ;

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
