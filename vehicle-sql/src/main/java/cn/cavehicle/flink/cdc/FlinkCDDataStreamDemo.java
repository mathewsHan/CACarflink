package cn.cavehicle.flink.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 基于DataStream实现Flink CDC案例演示：从MySQL数据库实时获取数据，打印控制台。
 */
public class FlinkCDDataStreamDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(3000);
		env.setParallelism(1);

		// 2. 数据源-source
		// 2-1. MySQL-CDC-Source
		MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
			.hostname("node1.itcast.cn")
			.port(3306)
			.databaseList("db_flink") // set captured database
			.tableList("db_flink.tbl_users") // set captured table
			.username("root")
			.password("123456")
			.startupOptions(StartupOptions.initial()) // 表示第1次读取表中数据，先全量加载，后增量加载
			.deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
			.build();
		// 2-2. 添加数据源
		DataStreamSource<String> logStream = env.fromSource(
			mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-cdc-source"
		);

		// 3. 数据转换-transformation

		// 4. 数据终端-sink
		logStream.printToErr();

		// 5. 触发执行-execute
		env.execute("FlinkCDDataStreamDemo");
	}

}  