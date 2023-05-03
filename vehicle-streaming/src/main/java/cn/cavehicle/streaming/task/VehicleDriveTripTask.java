package cn.cavehicle.streaming.task;

import cn.cavehicle.streaming.function.watermark.DriveTripWatermark;
import cn.cavehicle.streaming.function.window.DriverTripAnalysisWindowFunction;
import cn.cavehicle.streaming.function.window.DriverTripSampleWindowFunction;
import cn.cavehicle.entity.DriveTripReport;
import cn.cavehicle.entity.VehicleDataObj;
import cn.cavehicle.streaming.sink.DriveTripAnalysisToHBaseSink;
import cn.cavehicle.streaming.sink.DriveTripSampleToHBaseSink;
import cn.cavehicle.utils.JsonParseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 实时分析业务模块二：
 *      将上报车辆数据，过滤获取驾驶行程数据，划分每个车辆各个行程数据，对行程数据进行采样sample和分析report，最后存储到HBase表中
 */
public class VehicleDriveTripTask extends BaseTask{

	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root") ;

		// 1. 执行环境-env
		StreamExecutionEnvironment env = getEnv("VehicleDriveTripTask") ;
		env.setParallelism(1) ;  // 为了测试，设置并行度为1，并且消费topic数据从最新偏移量消费

		// 2. 数据源-source, todo: 从Kafka实时消费TBOX上报上报车辆数据
		DataStreamSource<String> kafkaStream = getKafkaStream(env, "trip-gid-1");
		// kafkaStream.printToErr("kafka");

		// 3. 数据转换-transformation
		/*
		todo: 3-1. 数据预处理
			a. json数据解析转换
			b. 过滤解析成功数据（获取正常数据）
			c. 过滤驾驶行程数据
				字段：chargeStatus，充电状态字段值为2（行程充电） 和3 （未充电）的车辆数据
		 */
		DataStream<VehicleDataObj> tripStream = kafkaStream
			// a. json数据解析转换
			.map(JsonParseUtil::parseJsonToBean)
			// b. 过滤解析成功数据（获取正常数据）
			.filter(object -> StringUtils.isEmpty(object.getErrorData()))
			// c. 过滤驾驶行程数据: chargeStatus = 2 | 3
			.filter(object -> (2 == object.getChargeStatus() || 3 == object.getChargeStatus()));
		tripStream.printToErr("trip");

		/*
		todo: 3-2. 驾驶行程数据划分行程，使用会话窗口
			将行驶数据，先按照vin车架号划分各个车辆行驶数据，然后设置会话窗口SessionWindow时间间隔gap划分各个车辆行程数据
			a. 设置事件时间字段及watermark
				允许最大乱序时间：30s，自定义Watermark实现
			b. 按照vin车架号分组
			c. 设置窗口大小：15min，划分行程
		 */
		WindowedStream<VehicleDataObj, String, TimeWindow> windowStream = tripStream
			// a. 设置事件时间字段及watermark
			.assignTimestampsAndWatermarks(new DriveTripWatermark())
			// b. 按照vin车架号分组
			.keyBy(VehicleDataObj::getVin)
			// c. 设置窗口大小：15min，划分行程
			.window(EventTimeSessionWindows.withGap(Time.minutes(15)));

		/*
		todo: 3-3. 各个驾驶行程数据采样
			自定义WindowFunction窗口函数，对各个行程中数据进行采样
		 */
		SingleOutputStreamOperator<String[]> sampleStream = windowStream.apply(new DriverTripSampleWindowFunction());
		//sampleStream.map(Arrays::toString).printToErr("sample");

		/*
		todo: 3-4. 各个驾驶行程数据分析
			自定义WindowFunction窗口函数，对行程指标进行统计
		 */
		SingleOutputStreamOperator<DriveTripReport> reportStream = windowStream.apply(new DriverTripAnalysisWindowFunction());
		reportStream.printToErr("report");

		// 4. 数据接收器-sink
		// todo: 4-1. 保存各个驾驶行程采样数据到HBase表中
		DriveTripSampleToHBaseSink sampleSink = new DriveTripSampleToHBaseSink("VEHICLE_NS:DRIVE_TRIP_SAMPLE");
		//sampleStream.addSink(sampleSink) ;

		// todo: 4-2. 保存各个驾驶行程统计的指标到HBase表中
		DriveTripAnalysisToHBaseSink reportSink = new DriveTripAnalysisToHBaseSink("VEHICLE_NS:DRIVE_TRIP_REPORT");
		reportStream.addSink(reportSink) ;

		// 5. 触发执行-execute
		env.execute("VehicleDriveTripTask") ;
	}


}
