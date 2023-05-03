package cn.cavehicle.test.jdbc;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

/**
 * 采用Flink JDBC方式，读取Hive表数据（SQL -> HiveServer2  ->  MapReduce）
 */
public class BatchJdbcHiveTest {

	//定义jdbc的链接地址
	private final static String URL = "jdbc:hive2://node1.itcast.cn:10000/?characterEncoding=utf8&useSSL=false";
	//定义用户名
	private final static String USER = "root";
	//定义密码
	private final static String PASSWORD = "123456";
	//定义驱动名称
	private final static String DRIVER_NAME="org.apache.hive.jdbc.HiveDriver";
	//定义sql语句
	//private final static String SQL = "SELECT json FROM vehicle_ods.vehicle_error LIMIT 10";
	private final static String SQL = "SELECT COUNT(1) AS srcTotalNum FROM vehicle_ods.vehicle_src";

	public static void main(String[] args) throws Exception {
		// 1. 获取执行环境
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2. 创建JDBCInputFormat对象
		JDBCInputFormat inputFormat = JDBCInputFormat
			.buildJDBCInputFormat()
			.setDrivername(DRIVER_NAME)
			.setDBUrl(URL)
			.setUsername(USER)
			.setPassword(PASSWORD)
			.setQuery(SQL)
			//.setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO))
			.setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.LONG_TYPE_INFO))
			.finish();

		// 3. 加载数据
		DataSource<Row> errorDataSet = env.createInput(inputFormat);

		// 4. 打印控制台
		errorDataSet.printToErr();
	}

}
