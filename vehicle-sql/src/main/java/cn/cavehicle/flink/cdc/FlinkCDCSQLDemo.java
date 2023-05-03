package cn.cavehicle.flink.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 基于Flink SQL实现Flink CDC案例演示：从MySQL数据库实时呼气数据，打印控制台
 */
public class FlinkCDCSQLDemo {

	public static void main(String[] args) {
		// 1. 获取表执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(3000);
		env.setParallelism(1);

		EnvironmentSettings settings = EnvironmentSettings
			.newInstance()
			.inStreamingMode()
			.useBlinkPlanner()
			.build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings) ;

		// 2. 创建输入表，todo：使用CDC-SQL-MySQL获取数据
		tableEnv.executeSql(
			"CREATE TABLE mysql_binlog (\n" +
				" id INT,\n" +
				" name STRING,\n" +
				" age INT,\n" +
				" gender STRING,\n" +
				" PRIMARY KEY(id) NOT ENFORCED\n" +
				") WITH (\n" +
				" 'connector' = 'mysql-cdc',\n" +
				" 'hostname' = 'node1.itcast.cn',\n" +
				" 'port' = '3306',\n" +
				" 'username' = 'root',\n" +
				" 'password' = '123456',\n" +
				" 'database-name' = 'db_flink',\n" +
				" 'table-name' = 'tbl_users',\n" +
				" 'scan.startup.mode' = 'initial'\n" +
				")"
		);

		// 3. 查询数据
		TableResult tableResult = tableEnv.executeSql("SELECT id, name, age, gender FROM mysql_binlog");

		// 4. 打印控制台
		tableResult.print();
	}

}
