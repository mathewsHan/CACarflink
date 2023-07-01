package cn.cavehicle.producer;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * 模拟数据：从本地文件系统读取文本文件数据，将其 实时 写入到Kafka Topic队列中
 * 2023-7-1
 * 暂时调试不通
 */
public class FlinkKafkaWriter {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//env.setParallelism(3) ;

		// 2. 数据源-source
		DataStreamSource<String> sourceStream = env.readTextFile("F:\\sourcedata.txt");

		// 3. 数据转换-transformation

		// 4. 数据接收器-sink
		// 4-1. 指定写入数据时序列化
		KafkaSerializationSchema<String> serializationSchema = new KafkaSerializationSchema<String>() {
			@Override
			public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
				return new ProducerRecord<>(
					"vehicle-data", // target topic
					element.getBytes(StandardCharsets.UTF_8)); // record contents
			}
		} ;
		// 4-2. 生产者属性设置
			Properties producerConfig = new Properties() ;
			producerConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.88.100:9092,192.168.88.101:9092,192.168.88.102:9092");
		// 4-3. 创建对象，传递参数
		FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
				"vehiclejsondata",
				new KafkaSerializationSchema<String>() {
					@Override
					public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
						return new ProducerRecord<>(
								"vehiclejsondata",
								element.getBytes()
						);
					}
				},
				producerConfig,
				FlinkKafkaProducer.Semantic.EXACTLY_ONCE
		);
		/*FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>(
			"vehicle-data", serializationSchema, producerConfig, FlinkKafkaProducer.Semantic.EXACTLY_ONCE
		);*/
		// 4-4. 添加接收器
		sourceStream.addSink(kafkaProducer) ;

		// 5. 触发执行-execute
		env.execute("FlinkKafkaWriter") ;
	}

}
