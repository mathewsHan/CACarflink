package cn.cavehicle.streaming.function.window;

import cn.cavehicle.entity.VehicleDataObj;
import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.LinkedList;

/**
 * 驾驶行程数据中各个行程数据采样，每隔5秒采样一条数据，并且获取每条数据中6个字段值
 *      todo: 输出数据类型为字符串数组String[], 数组中每条数据为行程中抽样的各条数据某个字段组合值
 *          终端时间字段：
 *              2019-11-20 15:10:39,2019-11-20 15:11:39,2019-11-20 15:12:19
 *          地理位置字段：
 *              107.86205|30.02505,107.86205|30.02505,107.86204|30.025
 */
public class DriverTripSampleWindowFunction
	implements WindowFunction<VehicleDataObj, String[], String, TimeWindow> {

	@Override
	public void apply(String key, TimeWindow window,
	                  Iterable<VehicleDataObj> input, Collector<String[]> out) throws Exception {
		// todo 1. 将迭代器中数据放到列表中，使用Google工具类：Guava
		LinkedList<VehicleDataObj> vehicleDataObjList = Lists.newLinkedList(input);

		// todo 2. 对列表中车辆数据按照终端时间升序排序
		vehicleDataObjList.sort(new Comparator<VehicleDataObj>() {
            @Override
            public int compare(VehicleDataObj o1, VehicleDataObj o2) {
                return o1.getTerminalTimeStamp().compareTo(o2.getTerminalTimeStamp());
            }
        });

		/*
		vin: 标识符
		行程开始时间 -> 行程中第1条数据时间
		    terminalTime: 终端时间
		    soc: 剩余电量百分比
		    mileage: 总里程数
		    speed: 速度
		    gps: 地理位置（经纬度）
		 */
		// todo 3. 定义数组，存储采样数据中各个字段的值
		String[] samples = new String[7] ;
		samples[0] = key ;

		// todo 4. 定义字符串，并且初始值为行程中第1条数据，得到对应字段的值
		VehicleDataObj firstVehicleDataObj = vehicleDataObjList.getFirst();
		// 设置行程中开始时间，行程中第1条数据终端时间
		samples[1] = firstVehicleDataObj.getTerminalTime() ;
		StringBuilder terminalTimeBuilder = new StringBuilder(firstVehicleDataObj.getTerminalTime());
		StringBuilder socBuilder = new StringBuilder(firstVehicleDataObj.getSoc() + "");
		StringBuilder mileageBuilder = new StringBuilder(firstVehicleDataObj.getTotalOdometer() + "");
		StringBuilder speedBuilder = new StringBuilder(firstVehicleDataObj.getSpeed() + "");
		StringBuilder gpsBuilder = new StringBuilder(firstVehicleDataObj.getLng() + "|" + firstVehicleDataObj.getLat());

		// todo 5. 遍历集合，周期性采样数据，相邻数据终端时间间隔大于5秒
		// 定义变量，存储上一条数据终端时间
		Long lastTerminalTimeStamp = firstVehicleDataObj.getTerminalTimeStamp();
		for (VehicleDataObj vehicleDataObj : vehicleDataObjList) {
			// 获取当前数据终端时间
			Long terminalTimeStamp = vehicleDataObj.getTerminalTimeStamp();
			// 当前数据不是行程第1条数据，并且与上一条数据时间间隔大于等于5秒
			if((!terminalTimeStamp.equals(lastTerminalTimeStamp))
				&& (Math.abs(terminalTimeStamp - lastTerminalTimeStamp) >= 5 * 1000L)
			){
				terminalTimeBuilder.append(",").append(vehicleDataObj.getTerminalTime());
				socBuilder.append(",").append(vehicleDataObj.getSoc());
				mileageBuilder.append(",").append(vehicleDataObj.getTotalOdometer());
				speedBuilder.append(",").append(vehicleDataObj.getSpeed()) ;
				gpsBuilder.append(",").append(vehicleDataObj.getLng()).append("|").append(vehicleDataObj.getLat());

				// 更新上一条数据时间戳
				lastTerminalTimeStamp = terminalTimeStamp;
			}
		}

		// todo 6. 设置数组中各个下标对应字段的值
		samples[2] = terminalTimeBuilder.toString();
		samples[3] = socBuilder.toString();
		samples[4] = mileageBuilder.toString();
		samples[5] = speedBuilder.toString();
		samples[6] = gpsBuilder.toString();

		// todo 7. 输出封装数据数据
		out.collect(samples);
	}

}
