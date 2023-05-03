package cn.cavehicle.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 依据经纬度，解析获取具体地理位置信息数据
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class VehicleLocationModel implements Serializable {
    //国家
    private String country;
    //省份
    private String province;
    //城市
    private String city;
    //区或县
    private String district;
    //镇
    private String township;
    //详细地址
    private String address;

    //纬度
    private Double lat = -999999D;
    //经度
    private Double lng = -999999D;
}