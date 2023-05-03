package cn.cavehicle.consumer;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * Flink 流式计算程序：从Kafka实时消费数据，使用FlinkKafkaConsumer实现
 */
public class FlinkKafkaReader {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(3) ;
		// todo: 设置检查点
		env.enableCheckpointing(5000) ;
		env.setStateBackend(new FsStateBackend("file:///D:/ckpts")) ;
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));

		// 2. 数据源-source
		// 2-1. Kafka 生产者属性设置
		Properties props = new Properties() ;
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1.itcast.cn:9092,node2.itcast.cn:9092,node3.itcast.cn:9092");
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "gid-vehicle-1");
		props.setProperty("flink.partition-discovery.interval-millis", "60000") ;
		// 2-2. 创建对象，传递参数
		FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(
			"vehicle-data", new SimpleStringSchema(), props
		);
		// todo: 设置从Kafka消费数据时起始偏移量
		kafkaConsumer.setStartFromEarliest() ;
		// 2-3. 添加数据源
		DataStreamSource<String> kafkaStream = env.addSource(kafkaConsumer);

		// 3. 数据转换-transformation

		// 4. 数据接收器-sink
		kafkaStream.printToErr("vehicle>");

		// 5. 触发执行-execute
		env.execute("FlinkKafkaReader") ;
	}

}
