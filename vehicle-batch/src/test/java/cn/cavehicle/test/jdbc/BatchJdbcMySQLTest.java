package cn.cavehicle.test.jdbc;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * 基于Flink JDBC方式，向MySQL数据库中写入数据和读取数据。
 */
public class BatchJdbcMySQLTest {

	//定义jdbc的链接地址
	private final static String URL = "jdbc:mysql://node1.itcast.cn:3306/?characterEncoding=utf8&useSSL=false";
	//定义用户名
	private final static String USER = "root";
	//定义密码
	private final static String PASSWORD = "123456";
	//定义驱动名称
	private final static String DRIVER_NAME = "com.mysql.jdbc.Driver";
	//定义sql语句
	private final static String SQL = "SELECT name, age, country FROM db_flink.user_info";
	private final static String INSERT_SQL = "INSERT INTO db_flink.user_info (name, age, country) VALUES(?, ?, ?)";

	public static void main(String[] args) throws Exception {
		// 1. 获取执行环境-env
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2. 向数据库写入数据
		//jdbcWriter(env) ;

		// 3. 从数据库读取数据
		jdbcReader(env) ;

		// 4. 触发执行
		env.execute("BatchJdbcMySQLTest");
	}

	/**
	 * 采用JDBCInputFormat方式从MySQL数据库表中读取数据
	 */
	private static void jdbcReader(ExecutionEnvironment env) throws Exception {
		// 1. 创建InputFormat对象
		RowTypeInfo rowTypeInfo = new RowTypeInfo(
			BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
		);

		JDBCInputFormat inputFormat = JDBCInputFormat.buildJDBCInputFormat()
			.setDrivername(DRIVER_NAME)
			.setDBUrl(URL)
			.setUsername(USER)
			.setPassword(PASSWORD)
			.setQuery(SQL)
			.setRowTypeInfo(rowTypeInfo)
			.finish();

		// 2. 加载数据
		DataSource<Row> dataSet = env.createInput(inputFormat);

		// 3. 打印数据到控制台
		dataSet.printToErr();
	}

	/**
	 * 采用JDBCOutputFormat方式将数据写入到MySQL数据库表中
	 */
	private static void jdbcWriter(ExecutionEnvironment env) {
		// 1. 模拟产生数据集
		List<Row> rowList = new ArrayList<>();
		Row firstRow = new Row(3);
		firstRow.setField(0, "张三");
		firstRow.setField(1, 30);
		firstRow.setField(2, "中国");
		rowList.add(firstRow);

		Row secondRow = new Row(3) ;
		secondRow.setField(0, "李四");
		secondRow.setField(1, 20);
		secondRow.setField(2, "中国");
		rowList.add(secondRow);

		DataSource<Row> rowDataSet = env.fromCollection(rowList);

		// 2. 创建OutputFormat对象
		JDBCOutputFormat outputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
			.setDrivername(DRIVER_NAME)
			.setDBUrl(URL)
			.setUsername(USER)
			.setPassword(PASSWORD)
			.setBatchInterval(1)
			.setQuery(INSERT_SQL)
			.finish();

		// 3. 写入数据
		rowDataSet.output(outputFormat);
	}


}
