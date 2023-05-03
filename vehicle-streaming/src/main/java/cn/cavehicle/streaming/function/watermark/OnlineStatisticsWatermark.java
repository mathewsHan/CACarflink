package cn.cavehicle.streaming.function.watermark;

import cn.cavehicle.entity.VehicleDataPartObj;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class OnlineStatisticsWatermark extends BoundedOutOfOrdernessTimestampExtractor<VehicleDataPartObj> {

	public OnlineStatisticsWatermark() {
		// 设置允许最大乱序时间
		super(Time.seconds(5));
	}

	@Override
	public long extractTimestamp(VehicleDataPartObj element) {
		// 获取车辆数据中终端时间
		return element.getTerminalTimeStamp();
	}
}
