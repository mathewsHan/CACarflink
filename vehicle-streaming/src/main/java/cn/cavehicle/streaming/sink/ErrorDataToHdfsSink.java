package cn.cavehicle.streaming.sink;

import cn.cavehicle.utils.ConfigLoader;
import cn.cavehicle.utils.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.json.JSONObject;

/**
 * 使用官方提供：StreamingFileSink实时将流式数据写入文件中，分区目录依据数据中terminalTime时间生成，如果没有字段，以当前系统日期为准
 */
public class ErrorDataToHdfsSink {

	/**
	 * 构建StreamingFileSink对象
	 */
	public static StreamingFileSink<String> getFileSink(){
		// a. 存储文件路径
		String outputPath = ConfigLoader.get("hdfsUri") + "/user/hive/warehouse/vehicle_ods.db/vehicle_data_error" ;
		// b. 创建FileSink对象，设置属性
		StreamingFileSink<String> sink = StreamingFileSink
			// 第1、设置数据存储路径和存储格式
			.<String>forRowFormat(
				new Path(outputPath), new SimpleStringEncoder<>("UTF-8")
			)
			// 第2、设置分桶，分区目录名称
			.withBucketAssigner(
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
					.withPartPrefix("vehicle-src")
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
				element：json字符串，要么解析异常，要么没有核心字段

				如果json正常，并且有terminalTime字段值，就获取日期；否则使用当前系统日期
			 */
			String terminalDate = DateUtil.getCurrentDate() ;

			try{
				// 解析JSON字符串
				JSONObject jsonObject = new JSONObject(element);
				// 获取终端时间
				if(jsonObject.keySet().contains("terminalTime")){
					String terminalTime = jsonObject.getString("terminalTime");
					if(StringUtils.isNotEmpty(terminalTime)){
						terminalDate = terminalTime.substring(0, 10) ;
					}
				}
			}catch (Exception e){
				System.out.println("异常车辆数据，解析JSON字符串异常，使用当前系统日期................");
			}

			// 返回分区ID
			return "dt=" + terminalDate;
		}

		@Override
		public SimpleVersionedSerializer<String> getSerializer() {
			return SimpleVersionedStringSerializer.INSTANCE;
		}
	}

}
