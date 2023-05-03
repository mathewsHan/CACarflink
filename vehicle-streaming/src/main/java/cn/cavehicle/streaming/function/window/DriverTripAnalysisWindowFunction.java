package cn.cavehicle.streaming.function.window;

import cn.cavehicle.entity.DriveTripReport;
import cn.cavehicle.entity.VehicleDataObj;
import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.LinkedList;

/**
 * 驾驶行程数据中各个行程数据，依据业务指标进行计算
 */
public class DriverTripAnalysisWindowFunction
	implements WindowFunction<VehicleDataObj, DriveTripReport, String, TimeWindow> {
	@Override
	public void apply(String key, TimeWindow window,
	                  Iterable<VehicleDataObj> input, Collector<DriveTripReport> out) throws Exception {
		// todo 1. 将迭代器中数据转存到列表List中
		LinkedList<VehicleDataObj> vehicleDataObjList = Lists.newLinkedList(input);

		// todo 2. 对列表中数据按照数据终端时间terminalTime进行升序排序
		vehicleDataObjList.sort(new Comparator<VehicleDataObj>() {
			@Override
			public int compare(VehicleDataObj o1, VehicleDataObj o2) {
				return o1.getTerminalTimeStamp().compareTo(o2.getTerminalTimeStamp());
			}
		});

		// todo 3. 对列表中数据计算指标，封装到对象中
		DriveTripReport driveTripReport = computeReport(key, vehicleDataObjList);

		// todo 4. 将行程分析结果数据输出
		out.collect(driveTripReport);
	}

	/**
	 * 对各个行程中数据进行指标计算，最后返回封装对象
	 */
	private DriveTripReport computeReport(String key, LinkedList<VehicleDataObj> vehicleDataObjList) {
		// a. 创建实体类对象
		DriveTripReport driveTripReport = new DriveTripReport() ;
		// b. 设置字段值
		// vin: 车架号
		driveTripReport.setVin(key);
		// tripStatus: 是否为异常行程,  0：正常行程 1：异常行程（只有一个采样点）
		if(vehicleDataObjList.size() > 1){
			driveTripReport.setTripStatus(0);
		}else {
			driveTripReport.setTripStatus(1);
		}

		// todo: 获取行程中第一条数据，设置相关属性
		VehicleDataObj firstVehicleDataObj = vehicleDataObjList.getFirst();
		// tripStartTime: 行程开始时间
		driveTripReport.setTripStartTime(firstVehicleDataObj.getTerminalTime());
		// startBmsSoc: 行程开始soc
		driveTripReport.setStartBmsSoc(firstVehicleDataObj.getSoc());
		// startLongitude: 行程开始经度
		driveTripReport.setStartLongitude(firstVehicleDataObj.getLng());
		// startLatitude: 行程开始纬度
		driveTripReport.setStartLatitude(firstVehicleDataObj.getLat());
		// startMileage: 行程开始里程
		driveTripReport.setStartMileage(firstVehicleDataObj.getTotalOdometer());

		// todo: 获取行程中最后一条数据，设置相关属性
		VehicleDataObj lastVehicleDataObj = vehicleDataObjList.getLast();
		// tripEndTime: 行程结束时间
		driveTripReport.setTripEndTime(lastVehicleDataObj.getTerminalTime());
		// endBmsSoc: 结束soc
		driveTripReport.setEndBmsSoc(lastVehicleDataObj.getSoc());
		// endLongitude: 结束经度
		driveTripReport.setEndLongitude(lastVehicleDataObj.getLng());
		// endLatitude: 结束纬度
		driveTripReport.setEndLatitude(lastVehicleDataObj.getLat());
		// endMileage: 结束里程
		driveTripReport.setEndMileage(lastVehicleDataObj.getTotalOdometer());

		// mileage: 行程里程消耗
		driveTripReport.setMileage(
			lastVehicleDataObj.getTotalOdometer() - firstVehicleDataObj.getTotalOdometer()
		);
		// timeConsumption: 行程消耗时间(分钟)
		driveTripReport.setTimeConsumption(
			(lastVehicleDataObj.getTerminalTimeStamp() - firstVehicleDataObj.getTerminalTimeStamp() ) / 1000 / 60.0
		);
		// socConsumption: soc消耗
		driveTripReport.setSocConsumption(
			firstVehicleDataObj.getSoc() - lastVehicleDataObj.getSoc() + 0.0
		);

		// 定义变量，存储行程中最高车速
		double maxSpeed = firstVehicleDataObj.getSpeed(); // 先赋值行程中第一条数据速度
		/*
			低速： 0 <= speed < 40
			中速： 40 <= speed < 80
			高速： 80 <= speed
		 */
		// 记录上一次soc
		int lastSoc = firstVehicleDataObj.getSoc();
		// 记录上一次里程
		double lastMileage = firstVehicleDataObj.getTotalOdometer();
		for (VehicleDataObj vehicleDataObj : vehicleDataObjList) {
			// 获每条数据中速度
			Double speed = vehicleDataObj.getSpeed();
			// todo: 笔记，获取最高速度
			maxSpeed = Math.max(maxSpeed, speed) ;
			// 计算soc消耗
			int socConsumer = Math.abs(lastSoc - vehicleDataObj.getSoc()) ;
			// 计算里程数
			double mileageTotal = vehicleDataObj.getTotalOdometer() - lastMileage ;

			// 低速： 次数、soc消耗和里程数
			if(speed >= 0 && speed < 40){
				// totalLowSpeedNums: 总低速的个数
				driveTripReport.setTotalLowSpeedNums(driveTripReport.getTotalLowSpeedNums() + 1);
				// lowBmsSoc: 低速soc消耗
				driveTripReport.setLowBmsSoc(driveTripReport.getLowBmsSoc() + socConsumer);
				// lowBmsMileage: 低速里程
				driveTripReport.setLowBmsMileage(driveTripReport.getLowBmsMileage() + mileageTotal);
			}

			// 中速： 次数、soc消耗和里程数
			if(speed >= 40 && speed < 80){
				// totalMediumSpeedNums: 总中速的个数
				driveTripReport.setTotalMediumSpeedNums(driveTripReport.getTotalMediumSpeedNums() + 1);
				// mediumBmsSoc: 中速soc消耗
				driveTripReport.setMediumBmsSoc(driveTripReport.getMediumBmsSoc() + socConsumer);
				// mediumBmsMileage: 中速里程
				driveTripReport.setMediumBmsMileage(driveTripReport.getMediumBmsMileage() + mileageTotal);
			}

			// 高速： 次数、soc消耗和里程数
			if(speed >= 80){
				// totalHighSpeedNums: 总高速个数
				driveTripReport.setTotalHighSpeedNums(driveTripReport.getTotalHighSpeedNums() + 1);
				// highBmsSoc: 高速soc消耗
				driveTripReport.setHighBmsSoc(driveTripReport.getHighBmsSoc() + socConsumer);
				// highBmsMileage: 高速里程
				driveTripReport.setHighBmsMileage(driveTripReport.getHighBmsMileage() + mileageTotal);
			}

			// todo: 每条数据处理完成以后，需要重新设置上一次soc和里程数
			lastSoc = vehicleDataObj.getSoc();
			lastMileage = vehicleDataObj.getTotalOdometer();
		}

		// maxSpeed: 最高行驶车速
		driveTripReport.setMaxSpeed(maxSpeed);

		// 返回计算指标对象
		return driveTripReport;
	}

}
