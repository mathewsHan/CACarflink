package cn.cavehicle.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 电子围栏数据保存结果实体类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ElectricFenceModel implements Comparable<ElectricFenceModel>{
	// 电子围栏结果表：id，主键
	private Long id = -999999L ;

	//车架号
	private String vin = "";
	//进电子围栏时间
	private String inTime = "";
	//出电子围栏时间
	private String outTime = "";

	//位置时间：yyyy-MM-dd HH:mm:ss
	private String gpsTime = "";
	//位置纬度
	private Double lat= -999999D;
	//位置经度
	private Double lng= -999999D;

	//电子围栏ID
	private int eleId = -999999;
	//电子围栏名称
	private String eleName = "";
	//中心点地址
	private String address = "";
	//中心点纬度
	private Double latitude = -999999D;
	//中心点经度
	private Double longitude = -999999D;
	//电子围栏半径
	private float radius = -999999F;

	//电子围栏状态：1 电子围栏内，0 电子围栏外
	private int fenceStatus = 0 ;

	//扩展中断时间戳，被使用窗口计算和数据排序
	private Long terminalTimestamp = -999999L;

	@Override
	public int compareTo(ElectricFenceModel o) {
		return this.getTerminalTimestamp().compareTo(o.getTerminalTimestamp());
	}

}