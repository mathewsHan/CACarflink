package cn.cavehicle.streaming.function.watermark;

import cn.cavehicle.entity.VehicleDataObj;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 自定义实现Watermark，针对驾驶行程分析中SessionWindow会话窗口设置：提取事件时间字段值和允许最大乱序时间
 */
public class DriveTripWatermark extends BoundedOutOfOrdernessTimestampExtractor<VehicleDataObj> {

	// 构造方法，传递允许最大乱序时间
	public DriveTripWatermark() {
		super(Time.seconds(30));
	}

	// 提取数据中事件时间字段值，必须Long类型，此处就是terminalTimestamp
	@Override
	public long extractTimestamp(VehicleDataObj element) {
		return element.getTerminalTimeStamp();
	}
}
