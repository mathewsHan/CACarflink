package cn.cavehicle.streaming.task;

import cn.cavehicle.streaming.function.flat.ElectricFenceDimensionConnectFunction;
import cn.cavehicle.streaming.function.watermark.ElectricFenceWatermark;
import cn.cavehicle.streaming.function.window.ElectricFenceMonitorWindowFunction;
import cn.cavehicle.streaming.task.source.MySQLElectricFenceDimensionSource;
import cn.cavehicle.entity.ElectricFenceDimension;
import cn.cavehicle.entity.ElectricFenceModel;
import cn.cavehicle.entity.VehicleDataPartObj;
import cn.cavehicle.streaming.sink.ElectricFenceMonitorToMySQLSink;
import cn.cavehicle.utils.JsonParsePartUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * 业务模块三：电子围栏分析
 *      对车辆数据流中数据，先解析封装过滤，获取正常数据，再与加载维度数据关联，如果监控车辆进出电子围栏，记录信息写入MySQL数据库
 */
public class VehicleElectricMonitorFenceTask extends BaseTask {

	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root") ;

		// 1. 执行环境-env
		StreamExecutionEnvironment env = getEnv("VehicleElectricFenceTask");
		env.setParallelism(1) ;

		// 2. 数据源-source
		DataStreamSource<String> kafkaStream = getKafkaStream(env, "fence-gid-1");
		// kafkaStream.printToErr("kafka") ;

		// 3. 数据转换-transformation
		/*
		todo: 3-1. 数据预处理
			a. 解析JSON字符串，封装实体类对象
			b. 过滤获取正常数据，errorData字段为空
		 */
		SingleOutputStreamOperator<VehicleDataPartObj> vehicleStream = kafkaStream
			// a. 解析JSON字符串，封装实体类对象
			.map(JsonParsePartUtil::parseJsonToObject)
			// b. 过滤获取正常数据，errorData字段为空
			.filter(object -> StringUtils.isEmpty(object.getErrorData()));
		//vehicleStream.printToErr("vehicle") ;

		/*
		todo: 3-2. 自定义数据源，加载MySQL维度数据并进行广播
			车辆数据 -> 大表数据流,  维表数据 -> 小表数据流, 维表数据有变化, 采用广播流方式，将维表数据广播, 存储在状态State
			a. 自定义数据源，获取MySQL数据库：电子围栏车辆表与电子围栏规则设置表拉宽数据
			b. 广播维表数据
			【自定义数据源，加载维表数据，必须时Map集合，考虑vin被多个电子围栏监控】
		 */
		DataStream<Map<String, List<ElectricFenceDimension>>> broadcastStream = env
			// a. 自定义数据源，获取MySQL数据库维度数据，封装Map集合中
			.addSource(new MySQLElectricFenceDimensionSource())
			// b. 广播维表数据
			.broadcast();
		broadcastStream.printToErr("broadcast") ;

		/*
		todo: 3-3. 将车辆数据流与广播流关联connect, 确定是否为监控数据，并且在电子围栏内还是外
			a. 使用connect连接2个流
			b. 对连接流进行处理, 判断和计算
		 */
		SingleOutputStreamOperator<ElectricFenceModel> modelStream = vehicleStream
			// a. 使用connect连接2个流
			.connect(broadcastStream)
			// b. 对连接流进行处理, 判断和计算
			.flatMap(new ElectricFenceDimensionConnectFunction());
		modelStream.printToErr("model") ;

		/*
		 todo: 3-4. 计算判断车辆驶入电子围栏还是驶出电子围栏, 使用窗口计算
		    a. 设置事件时间字段和水位线Watermark
		    b. 按照车架号vin分组
		        对每个车辆设置窗口计算
		    c. 设置窗口大小：基于事件时间滚动窗口
		    d. 定义窗口函数，确定驶入还是驶出电子围栏
		 */
		SingleOutputStreamOperator<ElectricFenceModel> windowStream = modelStream
			// a. 设置事件时间字段和水位线Watermark
			.assignTimestampsAndWatermarks(new ElectricFenceWatermark())
			// b. 按照车架号vin和电子围栏ID分组
			.keyBy(new KeySelector<ElectricFenceModel, Tuple2<String, Integer>>() {
				@Override
				public Tuple2<String, Integer> getKey(ElectricFenceModel value) throws Exception {
					return Tuple2.of(value.getVin(), value.getEleId());
				}
			})
			// c. 设置窗口大小：基于事件时间滚动窗口
			.window(TumblingEventTimeWindows.of(Time.seconds(90)))
			//  d. 定义窗口函数，确定驶入还是驶出电子围栏
			.apply(new ElectricFenceMonitorWindowFunction());
		windowStream.printToErr("window") ;


		// 4. 数据接收器-sink
		windowStream.addSink(new ElectricFenceMonitorToMySQLSink()) ;

		// 5. 触发执行-execute
		env.execute("VehicleElectricFenceTask");
	}

}
