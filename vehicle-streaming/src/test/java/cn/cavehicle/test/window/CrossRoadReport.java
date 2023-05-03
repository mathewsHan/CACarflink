package cn.cavehicle.test.window;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 定义实体类，封装各个窗口中卡口流量
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CrossRoadReport {

	private String windowStart ;
	private String windowEnd;
	private String roadId ;
	private int trafficFlowTotal ;

}
