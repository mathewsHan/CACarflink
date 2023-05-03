package cn.cavehicle.flink.connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 从Kafka Topic 中消费数据，基于Table API Connection连接器
 */
public class SQLConnectorHBaseSinkDemo {

	public static void main(String[] args) {
		// 1. 构建表执行环境-tEnv
		EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
		TableEnvironment tableEnv = TableEnvironment.create(settings) ;

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

		// 3. 定义输出表，将数据保存到HBase数据库表中
		tableEnv.executeSql(
			"CREATE TABLE tbl_log_hbase_sink (\n" +
				"   rowkey STRING,\n" +
				"   info Row<user_id STRING, item_id INTEGER, behavior STRING, ts STRING>,\n" +
				"   PRIMARY KEY (rowkey) NOT ENFORCED\n" +
				") WITH (\n" +
				"  'connector' = 'hbase-2.2',\n" +
				"  'table-name' = 'htbl_logs',\n" +
				"  'zookeeper.quorum' = 'node1.itcast.cn:2181,node2.itcast.cn:2181,node3.itcast.cn:2181',\n" +
				"  'zookeeper.znode.parent' = '/hbase',\n" +
				"  'sink.buffer-flush.max-size' = '1mb',\n" +
				"  'sink.buffer-flush.max-rows' = '1',\n" +
				"  'sink.buffer-flush.interval' = '1s',\n" +
				"  'sink.parallelism' = '3'\n" +
				")"
		);

		// 3. 编写SQL，直接查询表的数据
		/*
		INSERT INTO hTable
			SELECT rowkey, ROW(f1q1), ROW(f2q2, f2q3), ROW(f3q4, f3q5, f3q6) FROM T;
		 */
		tableEnv.executeSql(
			"INSERT INTO tbl_log_hbase_sink " +
				"SELECT CONCAT(user_id, '#', ts) AS rowkey , Row(user_id, item_id, behavior, ts) FROM tbl_log_kafka"
		);
	}

}
