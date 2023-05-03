package cn.cavehicle.test.window;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 定义实体类，封装卡口流数据
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CrossRoadEvent {

	private String trafficTime ;
	private String roadId ;
	private int trafficFlow ;

}
