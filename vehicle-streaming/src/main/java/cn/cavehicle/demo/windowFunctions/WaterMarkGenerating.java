package cn.cavehicle.demo.windowFunctions;

import akka.stream.actor.WatermarkRequestStrategy;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;

import java.time.Duration;


/* 数据乱序与数据延迟到达
 window
 allowLateness
 SideOutPut
 */
public class WaterMarkGenerating {

    public static void main(String[] args) {
      //  WatermarkStrategy.<Tuple2<Long,String>>forBoundedOutOfOrderdness(Duration.ofSeconds(20));
    }
}
