package cn.cavehicle.entity;

import cn.cavehicle.utils.DateUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 定义驾驶线程数据分析结果指标存储的Model对象
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DriveTripReport {
    //车架号
    private String vin = "";

    //行程开始时间
    private String tripStartTime = "";
    //行程开始soc
    private int startBmsSoc =  -999999;
    //行程开始经度
    private Double startLongitude = -999999D;
    //行程开始纬度
    private Double startLatitude =  -999999D;
    //行程开始里程
    private Double startMileage = -999999D;

    //结束soc
    private int endBmsSoc =  -999999;
    //结束经度
    private Double endLongitude =  -999999D;
    //结束纬度
    private Double endLatitude =  -999999D;
    //结束里程
    private Double endMileage =  -999999D;
    //行程结束时间
    private String tripEndTime = "" ;

    //行程里程消耗
    private Double mileage =  -999999D;
    //最高行驶车速
    private Double maxSpeed =  0D;
    //soc消耗
    private Double socConsumption =  0D;
    //行程消耗时间(分钟)
    private Double timeConsumption =  -999999D;

    //总低速的个数
    private Long totalLowSpeedNums = 0L;
    //总中速的个数
    private Long totalMediumSpeedNums =  0L;
    //总高速个数
    private Long totalHighSpeedNums =  0L;

    //低速soc消耗
    private Double lowBmsSoc =  0D;
    //中速soc消耗
    private Double mediumBmsSoc =  0D;
    //高速soc消耗
    private Double highBmsSoc =  0D;

    //低速里程
    private Double lowBmsMileage =  0D;
    //中速里程
    private Double mediumBmsMileage =  0D;
    //高速里程
    private Double highBmsMileage =  0D;

    //是否为异常行程 0：正常行程 1：异常行程（只有一个采样点）
    private int tripStatus = -999999;

    /**
     * 将驾驶行程计算结果数据保存到hdfs时候需要转换成可以被hive所识别的字符串格式
     */
    public String toHiveString() {
        StringBuilder resultString = new StringBuilder();
        if (!this.vin.equals("")) resultString.append(this.vin).append("\t"); else resultString.append("NULL").append("\t");
        if (!this.tripStartTime.equals("")) resultString.append(this.tripStartTime).append("\t"); else resultString.append("NULL").append("\t");
        if (!this.tripEndTime.equals("")) resultString.append(this.tripEndTime).append("\t"); else resultString.append("NULL").append("\t");
        if (this.startBmsSoc !=  -999999 ) resultString.append(this.startBmsSoc).append("\t"); else resultString.append("NULL").append("\t");
        if (this.startLongitude !=  -999999 ) resultString.append(this.startLongitude).append("\t"); else resultString.append("NULL").append("\t");
        if (this.startLatitude !=  -999999 ) resultString.append(this.startLatitude).append("\t"); else resultString.append("NULL").append("\t");
        if (this.startMileage !=  -999999 ) resultString.append(this.startMileage).append("\t"); else resultString.append("NULL").append("\t");
        if (this.endBmsSoc !=  -999999 ) resultString.append(this.endBmsSoc).append("\t"); else resultString.append("NULL").append("\t");
        if (this.endLongitude !=  -999999 ) resultString.append(this.endLongitude).append("\t"); else resultString.append("NULL").append("\t");
        if (this.endLatitude !=  -999999 ) resultString.append(this.endLatitude).append("\t"); else resultString.append("NULL").append("\t");
        if (this.endMileage !=  -999999 ) resultString.append(this.endMileage).append("\t"); else resultString.append("NULL").append("\t");
        if (this.mileage !=  -999999 ) resultString.append(this.mileage).append("\t"); else resultString.append("NULL").append("\t");
        if (this.maxSpeed !=  -999999 ) resultString.append(this.maxSpeed).append("\t"); else resultString.append("NULL").append("\t");
        if (this.socConsumption !=  -999999 ) resultString.append(this.socConsumption).append("\t"); else resultString.append("NULL").append("\t");
        if (this.timeConsumption !=  -999999 ) resultString.append(this.timeConsumption).append("\t"); else resultString.append("NULL").append("\t");
        if (this.totalLowSpeedNums !=  -999999 ) resultString.append(this.totalLowSpeedNums).append("\t"); else resultString.append("NULL").append("\t");
        if (this.totalMediumSpeedNums !=  -999999 ) resultString.append(this.totalMediumSpeedNums).append("\t"); else resultString.append("NULL").append("\t");
        if (this.totalHighSpeedNums !=  -999999 ) resultString.append(this.totalHighSpeedNums).append("\t"); else resultString.append("NULL").append("\t");
        if (this.lowBmsSoc !=  -999999 ) resultString.append(this.lowBmsSoc).append("\t"); else resultString.append("NULL").append("\t");
        if (this.mediumBmsSoc !=  -999999 ) resultString.append(this.mediumBmsSoc).append("\t"); else resultString.append("NULL").append("\t");
        if (this.highBmsSoc !=  -999999 ) resultString.append(this.highBmsSoc).append("\t"); else resultString.append("NULL").append("\t");
        if (this.lowBmsMileage !=  -999999 ) resultString.append(this.lowBmsMileage).append("\t"); else resultString.append("NULL").append("\t");
        if (this.mediumBmsMileage !=  -999999 ) resultString.append(this.mediumBmsMileage).append("\t"); else resultString.append("NULL").append("\t");
        if (this.highBmsMileage !=  -999999 ) resultString.append(this.highBmsMileage).append("\t"); else resultString.append("NULL").append("\t");
        if (this.tripStatus !=  -999999 ) resultString.append(this.tripStatus).append("\t"); else resultString.append("NULL").append("\t");
        resultString.append(DateUtil.getCurrentDateTime());

        return resultString.toString();
    }
}