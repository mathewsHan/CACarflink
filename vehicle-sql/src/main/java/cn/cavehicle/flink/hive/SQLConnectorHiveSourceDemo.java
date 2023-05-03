package cn.cavehicle.flink.hive;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * Flink SQL与Hive集成，创建HiveCatalog元数据对象，可以加载和读取数据
 */
public class SQLConnectorHiveSourceDemo {

	public static void main(String[] args) {

		// 1. 创建表执行环境
		EnvironmentSettings settings = EnvironmentSettings
			.newInstance()
			.inBatchMode()
			.useBlinkPlanner()
			.build();
		TableEnvironment tableEnv = TableEnvironment.create(settings);

		// 2. 创建HiveCatalog对象，传递配置参数
		HiveCatalog hiveCatalog = new HiveCatalog(
			"hiveCatalog",
			"default",
			"conf/hive-conf",
			"conf/hadoop-conf",
			"3.1.2"
		);
		// 注册Catalog
		tableEnv.registerCatalog("hive_catalog", hiveCatalog);
		// 使用Catalog
		tableEnv.useCatalog("hive_catalog");

		// 3. 编写DDL、DML和DQL依据
		tableEnv.executeSql("SHOW DATABASES").print();

		tableEnv.executeSql("SELECT * FROM db_hive.emp").print();
	}

}
