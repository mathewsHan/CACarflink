package cn.cavehicle.streaming.task;

import cn.cavehicle.streaming.function.async.AsyncGaodeHttpQueryFunction;
import cn.cavehicle.streaming.function.flat.OnlineDataConnectFunction;
import cn.cavehicle.streaming.function.map.LocationInfoMapFunction;
import cn.cavehicle.streaming.function.watermark.OnlineStatisticsWatermark;
import cn.cavehicle.streaming.function.window.OnlineStatisticsWindowFunction;
import cn.cavehicle.streaming.task.source.MySQLVehicleDimensionSource;
import cn.cavehicle.entity.OnlineDataModel;
import cn.cavehicle.entity.VehicleDataPartObj;
import cn.cavehicle.entity.VehicleDimension;
import cn.cavehicle.streaming.sink.OnlineDataToMySQLSink;
import cn.cavehicle.utils.JsonParsePartUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 远程诊断实时故障分析：
 *      从Kafka实时消费车辆数据，解析过滤获取正常数据，转换经纬度为地理信息，设置窗口统计指标，最后关联维度数据，保存MySQL数据库。
 */
public class VehicleOnlineStatisticsTask extends BaseTask{

	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root") ;

		// 1. 执行环境-env
		StreamExecutionEnvironment env = getEnv("VehicleOnlineStatisticsTask");
		env.setParallelism(1) ;  // 此处测试方便，设置并行度1

		// 2. 数据源-source
		DataStreamSource<String> kafkaStream = getKafkaStream(env, "online-gid-1");
		// kafkaStream.printToErr("kafka") ;

		// 3. 数据转换-transformation
		/*
		todo: 3-1. 数据预处理
			a. 解析JSON字符串，封装到实体类对象
			b. 过滤获取正常数据，依据errorData字段
			c. 过滤掉经纬度为空或-999999D数据
		 */
		SingleOutputStreamOperator<VehicleDataPartObj> vehicleStream = kafkaStream
			// a. 解析JSON字符串，封装到实体类对象
			.map(JsonParsePartUtil::parseJsonToObject)
			// b. 过滤获取正常数据，依据errorData字段
			.filter(object -> StringUtils.isEmpty(object.getErrorData()))
			// c. 过滤掉经纬度为空或-999999D数据
			.filter(
				object -> object.getLng() != -999999D && object.getLat() != -999999D
					&& StringUtils.isNotEmpty(object.getLng() + "") && StringUtils.isNotEmpty(object.getLat() + "")
			);
		vehicleStream.printToErr("vehicle") ;

		/*
		todo: 3-2. 查询Redis数据库, 获取地理位置信息
			a. 工具类RedisUtil
				key: 经纬度生成geohash值, value: 请求高德地图获取地理位置信息(json字符串）
			b. 使用map算子, 调用工具类获取地理信息
			c. 划分数据流
				withLocationStream 表示获取到地理位置数据
				noWithLocationStream 表示未获取到地理位置数据, 需要异步请求GaoDeAPI
		 */
		// b. 使用map算子, 调用工具类获取地理信息
		DataStream<VehicleDataPartObj> mapStream = vehicleStream.map(new LocationInfoMapFunction());
		// c. 划分数据流
		DataStream<VehicleDataPartObj> redisLocationStream = mapStream.filter(object -> StringUtils.isNotEmpty(object.getProvince()));
		DataStream<VehicleDataPartObj> noLocationStream = mapStream.filter(object -> StringUtils.isEmpty(object.getProvince()));
		redisLocationStream.printToErr("redis") ;


		/*
		todo: 3-3. 异步请求高德地图API, 获取地理信息
			 a. 调用异步工具类，实现异步请求
			 b. 创建异步函数，其中解析请求向JSON字符串，封装到实体类对象
			 c. 保存地理位置信息数据到Redis中
		 */
		DataStream<VehicleDataPartObj> gaodeLocationStream = AsyncDataStream.unorderedWait(
			noLocationStream, // 数据流，对其中每条数据异步请求处理数据
			new AsyncGaodeHttpQueryFunction(), // 异步处理函数
			30000, TimeUnit.MILLISECONDS, // 异步请求超时时间
			10
		);
		gaodeLocationStream.printToErr("gaode") ;

		/*
		todo： 3-4. 设置窗口，计算指标（远程诊断车辆状况和报警故障记录）
			a. 合并位置数据流locationStream
			b. 设置事件时间字段和watermark
			c. 按照车架号vin分组
			d. 设置窗口大小和窗口类型
			e. 定义窗口函数，指标计算
		 */
		// a. 合并位置数据流locationStream
		DataStream<VehicleDataPartObj> locationStream = redisLocationStream.union(gaodeLocationStream);
		// todo: 对合并车辆数据流（已经包含位置信息）设置窗口，进行指标计算
		SingleOutputStreamOperator<OnlineDataModel> windowStream = locationStream
			// b. 设置事件时间字段和watermark
			.assignTimestampsAndWatermarks(new OnlineStatisticsWatermark())
			//  c. 按照车架号vin分组
			.keyBy(VehicleDataPartObj::getVin)
			//  d. 设置窗口大小和窗口类型
			.window(TumblingEventTimeWindows.of(Time.seconds(30)))
			//  e. 定义窗口函数，指标计算
			.apply(new OnlineStatisticsWindowFunction());
		windowStream.printToErr("window") ;

		/*
		todo: 3-5. 关联车辆维度信息数据
			a. 自定义数据源，加载维度数据并广播
			b. 使用connect连接2个流
			c. 自定义函数，实现数据关联操作
			【与电子围栏分析中，加载电子围栏监控车辆数据和对应围栏信息数据完全一致】
		 */
		// a. 自定义数据源，加载维度数据并广播
		DataStream<Map<String, VehicleDimension>> broadcastStream = env.addSource(new MySQLVehicleDimensionSource()).broadcast();
		broadcastStream.printToErr("broadcast") ;

		SingleOutputStreamOperator<OnlineDataModel> resultStream = windowStream
			// b. 使用connect连接2个流
			.connect(broadcastStream)
			// c. 自定义函数，实现数据关联操作
			.flatMap(new OnlineDataConnectFunction());
		resultStream.printToErr("result") ;

		// 4. 数据接收器-sink
		resultStream.addSink(new OnlineDataToMySQLSink()) ;

		// 5. 触发执行-execute
		env.execute("VehicleOnlineStatisticsTask") ;
	}

}
