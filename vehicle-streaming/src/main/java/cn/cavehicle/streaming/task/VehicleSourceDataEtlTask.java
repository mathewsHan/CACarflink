package cn.cavehicle.streaming.task;

import cn.cavehicle.entity.VehicleDataObj;
import cn.cavehicle.streaming.sink.ErrorDataToHdfsSink;
import cn.cavehicle.streaming.sink.SrcDataToHdfsSink;
import cn.cavehicle.streaming.sink.SrcDetailDataToPhoenixSink;
import cn.cavehicle.utils.JsonParseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 业务一：原始数据实时ETL
 *      todo: 从Kafka实时消费车辆数据（json字符串）, 然后调用工具类解析JSON字符串，将其封装到实体类对象，并且判断数据是正常数据还是异常数据，分别存储到HDFS和HBase。
 */
public class VehicleSourceDataEtlTask extends BaseTask{

	public static void main(String[] args) throws Exception{
		System.setProperty("HADOOP_USER_NAME", "root") ;

		// 1. 执行环境-env， todo: 流计算执行环境
		StreamExecutionEnvironment env = getEnv("VehicleSourceDataTask");
		// todo: 并行度 -> 依据消费topic队列分区partition数目而定
		env.setParallelism(3) ;

		// 2. 数据源-source, todo: 消费Kafka中车辆数据【vehicle-data】
		DataStream<String> kafkaStream = getKafkaStream(env, "etl-gid-1");
		kafkaStream.printToErr("kafka");

		// 3. 数据转换-transformation
		// 3-1. 解析JSON字符串为实体类对象
		DataStream<VehicleDataObj> objectStream = kafkaStream.map(JsonParseUtil::parseJsonToBean);
		//objectStream.printToErr("object");

		// 3-2. 过滤获取正常数据和异常数据，todo：使用侧边流输出异常数据
		// a. 定义侧边流标签
		OutputTag<VehicleDataObj> errorOutputTag = new OutputTag<VehicleDataObj>("vehicle-error"){} ;
		// b. 使用process算子处理数据
		SingleOutputStreamOperator<VehicleDataObj> srcStream = objectStream.process(
			new ProcessFunction<VehicleDataObj, VehicleDataObj>() {
				@Override
				public void processElement(VehicleDataObj vehicleDataObj,
				                           Context ctx, Collector<VehicleDataObj> out) throws Exception {
					// 获取异常数据字段值
					String errorData = vehicleDataObj.getErrorData();

					// todo：errorData为空时，属于正常数据
					if(StringUtils.isEmpty(errorData)){
						out.collect(vehicleDataObj);
					}else{
						// todo: errorData不为空时，数据为异常数据，进行侧边输出
						ctx.output(errorOutputTag, vehicleDataObj);
					}
				}
			}
		);

		// 4. 数据接收器-sink，todo：存储到hdfs和hbase中
		// 4-1. 正常数据保存到HDFS
		StreamingFileSink<String> srcFileSink = SrcDataToHdfsSink.getFileSink();
		//srcStream.map(VehicleDataObj::toHiveString).addSink(srcFileSink) ;

		// 4-2. 正常数据保存HBase（常用字段），由于最后与Phoenix关联，直接采用JDBC方式，将数据写入Phoenix表中
		SrcDetailDataToPhoenixSink detailPhoenixSink = new SrcDetailDataToPhoenixSink();
		srcStream.addSink(detailPhoenixSink) ;

		// 4-3. 异常数据保存HDFS（侧边流）
		DataStream<VehicleDataObj> errorStream = srcStream.getSideOutput(errorOutputTag);
		StreamingFileSink<String> errorFileSink = ErrorDataToHdfsSink.getFileSink();
		//errorStream.map(VehicleDataObj::getErrorData).addSink(errorFileSink).setParallelism(1) ;

		// 5. 触发执行-execute
		env.execute("VehicleSourceDataEtlTask") ;
	}

}
