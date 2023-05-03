package cn.cavehicle.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 定义车辆基础信息表的数据模型对象
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class VehicleDimension {
	//车架号
	private String vin;
	//车型编码
	private String modelCode;
	//车辆类型全名
	private String modelName;
	//车系编码
	private String seriesCode;
	//车系名称
	private String seriesName;
	//出售日期
	private String salesDate;
	//车辆用途
	private String carType;
	//车辆类型简称
	private String nickName;
	//年限
	private String liveTime;
}