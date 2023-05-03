package cn.cavehicle.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * MySQL数据库中：电子围栏车辆表与电子围栏规则设置表关联数据封装实体类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ElectricFenceDimension {

	// 电子围栏id
	private int id ;
	// 电子围栏名称
	private String name ;
	// 电子围栏地址
	private String address ;
	// 电子围栏半径
	private float radius ;
	// 电子围栏中心点经纬度
	private double longitude ;
	private double latitude ;

	// 电子围栏监控开始和结束时间
	private Date startTime ;
	private Date endTime ;

}