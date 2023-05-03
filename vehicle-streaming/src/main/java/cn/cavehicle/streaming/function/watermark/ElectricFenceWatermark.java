package cn.cavehicle.streaming.function.watermark;

import cn.cavehicle.entity.ElectricFenceModel;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 自定义水位线Watermark，继承官方BoundedOutOfOrdernessTimestampExtractor抽象类，设置允许最大乱序时间和数据中事件时间字段内置
 */
public class ElectricFenceWatermark extends BoundedOutOfOrdernessTimestampExtractor<ElectricFenceModel> {

	public ElectricFenceWatermark() {
		// 设置允许最大乱序时间
		super(Time.seconds(1));
	}

	@Override
	public long extractTimestamp(ElectricFenceModel element) {
		// 获取车辆数据中终端时间
		return element.getTerminalTimestamp();
	}
}
