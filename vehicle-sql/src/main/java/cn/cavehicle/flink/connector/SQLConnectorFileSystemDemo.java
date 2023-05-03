package cn.cavehicle.flink.connector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 从Kafka Topic 中消费数据，基于Table API Connection连接器
 */
public class SQLConnectorFileSystemDemo {

	public static void main(String[] args) {
		// 1. 构建表执行环境-tEnv
		//EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

		Configuration configuration = new Configuration() ;
		configuration.setString("execution.checkpointing.interval", "10000");
		configuration.setString("execution.runtime-mode", "streaming");
		configuration.setString("table.planner", "blink");
		TableEnvironment tableEnv = TableEnvironment.create(configuration) ;

		// 2. 定义输入表，从Kafka消费数据
		tableEnv.executeSql(
			"CREATE TABLE tbl_log_kafka (\n" +
				"  `user_id` STRING,\n" +
				"  `item_id` INTEGER,\n" +
				"  `behavior` STRING,\n" +
				"  `ts` STRING\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'log-topic',\n" +
				"  'properties.bootstrap.servers' = 'node1.itcast.cn:9092,node2.itcast.cn:9092,node3.itcast.cn:9092',\n" +
				"  'properties.group.id' = 'gid-1',\n" +
				"  'scan.startup.mode' = 'latest-offset',\n" +
				"  'format' = 'csv'\n" +
				")"
		);

		// 3. 定义输出表，将数据保存到文件系统中，数据存储格式：parquet
		tableEnv.executeSql(
			"CREATE TABLE tbl_log_fs_sink (\n" +
				"  `user_id` STRING,\n" +
				"  `item_id` INTEGER,\n" +
				"  `behavior` STRING,\n" +
				"  `ts` STRING\n" +
				") WITH (\n" +
				"  'connector' = 'filesystem',\n" +
				"  'path' = 'datas/track-logs',\n" +
				"  'format' = 'parquet',\n" +
				"  'sink.parallelism' = '1',\n" +
				"  'sink.rolling-policy.file-size' = '2MB',\n" +
				"  'sink.rolling-policy.rollover-interval' = '1 min',\n" +
				"  'sink.rolling-policy.check-interval' = '1 min'\n" +
				")"
		);

		// 3. 编写SQL，直接查询表的数据
		tableEnv.executeSql(
			"INSERT INTO tbl_log_fs_sink SELECT user_id, item_id, behavior, ts FROM tbl_log_kafka"
		);
	}

}
