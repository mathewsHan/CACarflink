package cn.cavehicle.streaming.task;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;
import java.util.Properties;

/**
 * Flink 流式计算任务Task基类，包含2个封装公共方法：
 *      1. 创建流式执行环境，并且设置属性值（Checkpoint、StateBackend和重启策略）
 *      2. 获取Kafka数据流，消费topic中数据
 */
public abstract class BaseTask {

	// 加载项目属性配置你文件，使用Flink中工具类ParameterTool读取
	public static ParameterTool parameterTool = null ;
	static{
		try {
			parameterTool = ParameterTool.fromPropertiesFile(
				BaseTask.class.getClassLoader().getResourceAsStream("config.properties")
			);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 初始化执行环境，进行相关设置
	 */
	protected static StreamExecutionEnvironment getEnv(String taskName) throws Exception {
		// 创建流式执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// todo: 设置加载属性文件中数据ParameterTool对象为应用Job全局参数，可以在任意地方获取
		env.getConfig().setGlobalJobParameters(parameterTool);

		// todo: 设置流式应用中，如果基于时间窗口计算，全部都是基于事件事假EventTime
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// todo: 设置Checkpoint检查点相关属性
		// a. 启用Checkpoint，设置时间间隔
		env.enableCheckpointing(30 * 1000L) ;
		// b. 设置状态后端StateBackend
		String checkpointDataDir = parameterTool.getRequired("hdfsUri") + "/flink-checkpoints/" + taskName;
		env.setStateBackend(
			new FsStateBackend(checkpointDataDir)
		);
		// c. 设置2个Checkpoint时间间隔
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500L);
		// d. 设置Job取消时，Checkpoint目录数据状态：保留
		env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		// e. 设置Checkpoint最大失败次数
		env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
		// f. 设置Checkpoint超时时间
		env.getCheckpointConfig().setCheckpointTimeout(10 * 60 * 1000L);
		// g. 设置同一时刻并行运行Checkpoint个数
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		// h. 设置检查点时数据语义性
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

		// todo: 设置重启策略
		env.setRestartStrategy(
			RestartStrategies.fixedDelayRestart(3, Time.seconds(10))
		);

		// 返回流式执行环境
		return env ;
	}

	/**
	 * 实时消费Kafka中数据，使用Connector类：FlinkKafkaConsumer，设置属性值
	 */
	protected static DataStreamSource<String> getKafkaStream(StreamExecutionEnvironment env, String groupId) {
		// a. 获取Topic名称
		String topicName = parameterTool.getRequired("kafka.topic");

		// b. 消费Kafka数据时属性设置
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, parameterTool.getRequired("bootstrap.servers"));
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		// todo: 设置topic分区发现
		props.setProperty(
			"flink.partition-discovery.interval-millis",
			parameterTool.get("kafka.partition.discovery.millionseconds", "60000")
		);

		// c. 创建Consumer对象
		FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(topicName, new SimpleStringSchema(), props);
		// todo: 设置消费起始偏移量
		// consumer.setStartFromEarliest();
		consumer.setStartFromLatest();

		// d. 添加数据源，获取数据流
		DataStreamSource<String> kafkaStream = env.addSource(consumer);

		// 返回获取数据流
		return kafkaStream ;
	}


}
