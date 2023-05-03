package cn.cavehicle.streaming.task;

import cn.cavehicle.utils.DateUtil;
import cn.cavehicle.entity.VehicleDataObj;
import cn.cavehicle.streaming.sink.SrcDataToHBaseBulkSink;
import cn.cavehicle.streaming.sink.SrcDetailDataToHBaseSink;
import cn.cavehicle.utils.JsonParseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

/**
 * 业务一：原始数据实时ETL
 *      todo: 从Kafka实时消费车辆数据（json字符串）, 然后调用工具类解析JSON字符串，将其封装到实体类对象，并且判断数据是正常数据还是异常数据，分别存储到HDFS和HBase。
 */
public class VehicleSourceDataTask extends BaseTask{

	public static void main(String[] args) throws Exception{
		System.setProperty("HADOOP_USER_NAME", "root") ;

		// 1. 执行环境-env， todo: 流计算执行环境
		StreamExecutionEnvironment env = getEnv("VehicleSourceDataTask");
		// todo: 并行度 -> 依据消费topic队列分区partition数目而定
		env.setParallelism(3) ;

		// 2. 数据源-source, todo: 消费Kafka中车辆数据【vehicle-data】
		DataStream<String> kafkaStream = getKafkaStream(env, "vehicle-gid-4");
		//kafkaStream.printToErr("kafka>");

		// 3. 数据转换-transformation
		// 3-1. 解析JSON字符串为实体类对象
		DataStream<VehicleDataObj> objectStream = kafkaStream.map(JsonParseUtil::parseJsonToBean);
		//objectStream.printToErr("object>");

		// 3-2. 过滤获取正常数据
		DataStream<VehicleDataObj> srcStream = objectStream.filter(
			vehicleDataObj -> StringUtils.isEmpty(vehicleDataObj.getErrorData())
		);
		//srcStream.print("src>");

		// 3-3. 过滤获取异常数据
		DataStream<VehicleDataObj> errorStream = objectStream.filter(
			vehicleDataObj -> StringUtils.isNotEmpty(vehicleDataObj.getErrorData())
		);
		//errorStream.printToErr("error>");

		// 4. 数据接收器-sink
		// 4-1. 正常数据存储HDFS
		StreamingFileSink<String> srcFileSink = getFileSink("vehicle_src");
		//srcStream.map(VehicleDataObj::toHiveString).addSink(srcFileSink) ;

		// 4-2. 异常数据存储HDFS
		//StreamingFileSink<String> errorFileSink = getFileSink("vehicle_error");
		//errorStream.map(VehicleDataObj::getErrorData).addSink(errorFileSink).setParallelism(1) ; // 算子基本并行度
		// todo: 采用自动Sink将数据写入到分区目录中
		//errorStream.map(VehicleDataObj::getErrorData).addSink(new ErrorDataToHiveSink()).setParallelism(1) ;

		// 4-3. 正常数据存储HBase（所有字段）
		//SrcDataToHBaseSink hBaseSink = new SrcDataToHBaseSink("VEHICLE_NS:VEHICLE_SRC") ;
		//srcStream.addSink(hBaseSink);
		// todo: 批量插入车辆数据到HBase表中
		SrcDataToHBaseBulkSink bulkHbaseSink = new SrcDataToHBaseBulkSink("VEHICLE_NS:VEHICLE_SRC_BULK") ;
		//srcStream.addSink(bulkHbaseSink) ;

		// 4-4. 正常数据存储HBase（常用字段）
		SrcDetailDataToHBaseSink detailHbaseSink = new SrcDetailDataToHBaseSink("VEHICLE_NS:VEHICLE_SRC_DETAIL");
		srcStream.addSink(detailHbaseSink) ;

		// 5. 触发执行-execute
		env.execute("VehicleSourceDataTask") ;
	}

	/**
	 * 采用官方提供StreamingFileSink 实时将流式数据写入文件中，传入写入路径（表名称）
	 */
	private static StreamingFileSink<String> getFileSink(String tableName){
		// a. 存储文件路径
		String outputPath = parameterTool.getRequired("hdfsUri") + "/user/hive/warehouse/vehicle_ods.db/" + tableName ;
		// b. 创建FileSink对象，设置属性
		StreamingFileSink<String> sink = StreamingFileSink
			// 第1、设置数据存储路径和存储格式
			.<String>forRowFormat(
				new Path(outputPath), new SimpleStringEncoder<>("UTF-8")
			)
			// 第2、设置分桶，分区目录名称
			.withBucketAssigner(
				//new DateTimeBucketAssigner<>("yyyyMMdd")
				new HivePartitionBucketAssigner()
			)
			// 第3、设置文件滚动策略
			.withRollingPolicy(
				DefaultRollingPolicy.builder()
					.withMaxPartSize(128 * 1024 * 1024)
					.withRolloverInterval(30 * 1000L)
					.withInactivityInterval(30 * 1000L)
					.build()
			)
			// 第4、文件名称设置
			.withOutputFileConfig(
				OutputFileConfig.builder()
					.withPartPrefix("vehicle")
					.withPartSuffix(".data")
					.build()
			)
			.build();
		// 3. 返回对象
		return sink ;
	}

	/**
	 * 自定义StreamingFileSink中分桶策略，实现与Hive中分区路径一致：dt=2022-05-14
	 */
	private static class HivePartitionBucketAssigner implements BucketAssigner<String, String> {
		@Override
		public String getBucketId(String element, Context context) {
			/*
				数据所属分区，属于多级分区，值从字段中获取
				a. 从数据element中获取字段的值
				b. 拼凑字符串
					year=2022/province=上海市
			 */
			// 获取当前系统时间
			return "dt=" + DateUtil.getCurrentDate();
		}

		@Override
		public SimpleVersionedSerializer<String> getSerializer() {
			return SimpleVersionedStringSerializer.INSTANCE;
		}
	}

}
