package cn.cavehicle.test.window;

import lombok.SneakyThrows;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 基于事件时间窗口统计分析案例：卡口流量实时统计，窗口大小=10分钟
 */
public class TrafficCrossRoadWindowTest {

	public static void main(String[] args) throws Exception {
		// 1. 获取流式执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(10 * 1000L) ;

		// todo: 第1步、设置时间语义：EventTime事件时间
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// 2. 数据源-source
		DataStreamSource<String> lineStream = env.socketTextStream("node1.itcast.cn", 9999);
/*
2022-04-15 09:01:00,r1,10
2022-04-15 09:04:00,r1,10
2022-04-15 09:05:00,r1,10
2022-04-15 09:06:00,r1,10
2022-04-15 09:10:00,r1,10

2022-04-15 09:09:58,r1,10

2022-04-15 09:11:00,r1,10

2022-04-15 09:09:57,r1,10
2022-04-15 09:13:00,r1,10

2022-04-15 09:09:59,r1,10
 */
		// 3. 数据转换-transformation
		SingleOutputStreamOperator<String> timeStream = lineStream
			.filter(line -> line.trim().split(",").length == 3)
			// todo: 第2步、指定数据中事件时间字段值，并且Long类型
			.assignTimestampsAndWatermarks(
				// todo: 设置允许最大数据乱序时间： 1m，不能太大，如果太大，窗口计算没有实时性
				new BoundedOutOfOrdernessTimestampExtractor<String>(Time.minutes(1)) {
					private FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

					@SneakyThrows
					@Override
					public long extractTimestamp(String element) {
						System.out.println("element: " + element);

						String eventTime = element.split(",")[0];
						long timestamp = format.parse(eventTime).getTime();
						return timestamp;
					}
				}
			);

		// 3-1. 解析数据，封装流量数据到实体类对象
		SingleOutputStreamOperator<CrossRoadEvent> eventStream = timeStream.map(new MapFunction<String, CrossRoadEvent>() {
			@Override
			public CrossRoadEvent map(String value) throws Exception {
				String[] array = value.split(",");
				CrossRoadEvent crossRoadEvent = new CrossRoadEvent();
				crossRoadEvent.setTrafficTime(array[0]);
				crossRoadEvent.setRoadId(array[1]);
				crossRoadEvent.setTrafficFlow(Integer.parseInt(array[2]));
				// 返回封装实体类对象
				return crossRoadEvent;
			}
		});
		
		// 3-2. todo： 第3步、设置窗口和函数
		OutputTag<CrossRoadEvent> lateOutputTag = new OutputTag<CrossRoadEvent>("late-data"){} ;
		SingleOutputStreamOperator<CrossRoadReport> windowStream = eventStream
			// a. 按照卡口分组
			.keyBy(CrossRoadEvent::getRoadId)
			// b. 设置窗口大小：10分钟
			.window(
				TumblingEventTimeWindows.of(Time.minutes(10))
			)
			// todo: 设置最大允许延迟时间：2m, 此时窗口已经触发计算，将窗口状态保存2m时间，在时间范围内，如果有窗口延迟数据达到，将会再次触发窗口计算
			.allowedLateness(Time.minutes(2))
			// todo: 迟到数据所在窗口已经销毁，此时可以将迟到数据放到侧边流中，后续单独处理
			.sideOutputLateData(lateOutputTag)
			// c. 窗口数据计算
			.apply(new WindowFunction<CrossRoadEvent, CrossRoadReport, String, TimeWindow>() {
				private FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
				@Override
				public void apply(String roadId, // 分组卡口编号
				                  TimeWindow window, // 时间窗口，获取窗口开始时间和结束时间
				                  Iterable<CrossRoadEvent> input, // 窗口中数据，放在迭代器中
				                  Collector<CrossRoadReport> out) throws Exception {
					// a. 获取窗口时间
					String windowStart = this.format.format(window.getStart());
					String windowEnd = this.format.format(window.getEnd()) ;

					// b. 窗口中数据计算
					int total = 0 ;
					for (CrossRoadEvent crossRoadEvent : input) {
						total = total + crossRoadEvent.getTrafficFlow() ;
					}

					// c. 创建实例对象，封装计算结果
					CrossRoadReport crossRoadReport = new CrossRoadReport();
					crossRoadReport.setWindowStart(windowStart);
					crossRoadReport.setWindowEnd(windowEnd);
					crossRoadReport.setRoadId(roadId);
					crossRoadReport.setTrafficFlowTotal(total);

					// d. 输出结果
					out.collect(crossRoadReport);
				}
			});

		// 4. 数据接收器-sink
		windowStream.printToErr();

		// todo: 依据标签获取侧边流中迟到数据
		DataStream<CrossRoadEvent> lateStream = windowStream.getSideOutput(lateOutputTag);
		lateStream.print("late"); // 企业中方式，保存到存储引擎，比如HDFS文件或HBase表中，后期单独分析为啥迟到很久

		// 5. 触发执行-execute
		env.execute("TrafficCrossRoadWindowTest") ;
	}

}
