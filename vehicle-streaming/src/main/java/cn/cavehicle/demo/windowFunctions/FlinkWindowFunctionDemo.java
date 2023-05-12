package cn.cavehicle.demo.windowFunctions;

/*
滚动窗口
滑动窗口
回话窗口
 Trigger 一共8个内置触发
  ProcessingTimeTrigger
  EventTimeTrigger
  ContinuousProcessingTimeTrigger   提前获得当前窗口阶段聚合结果
  ContinuousEventTimeTrigger
    CountTrigger    countWindow默认触发器
    NeverTrigger
    PurgingTrigger  清除触发器, 触发器触发后清除已经计算好的state
    DeltaTrigger  上一次触发计算的元素与当前元素之间间隔超过阈值
 ---Evictor
 CountEvict
 TimeEvict  只保留N段时间范围内的Element
 DeltaEvict

触发器决定了窗口的触发机制，驱逐器决定了窗口内的数据删除的机制
 */
public class FlinkWindowFunctionDemo {



}
