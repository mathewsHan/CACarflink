package cn.cavehicle.utils;

import org.gavaghan.geodesy.Ellipsoid;
import org.gavaghan.geodesy.GeodeticCalculator;
import org.gavaghan.geodesy.GeodeticCurve;
import org.gavaghan.geodesy.GlobalCoordinates;

import java.util.Objects;

/**
 * 球面距离计算工具类：根据两个点的经纬度，计算出距离
 */
public class DistanceCaculateUtil {
    /**
     * 计算地址位置方法：坐标系、经纬度用于计算距离(直线距离)
     */
    private static Double getDistanceMeter(GlobalCoordinates gpsFrom, GlobalCoordinates gpsTo, Ellipsoid ellipsoid) {
        //
        GeodeticCurve geodeticCurve = new GeodeticCalculator().calculateGeodeticCurve(ellipsoid, gpsFrom, gpsTo);
        return geodeticCurve.getEllipsoidalDistance();
    }

    /**
     * 使用传入的ellipsoidsphere方法计算距离
     */
    private static Double ellipsoidMethodDistance(Double latitude, Double longitude, Double latitude2, Double longitude2, Ellipsoid ellipsoid){
        // todo 位置点经度、维度不为空 位置点2经度、维度不为空 椭圆算法
        Objects.requireNonNull(latitude, "latitude is not null");
        Objects.requireNonNull(longitude, "longitude is not null");
        Objects.requireNonNull(latitude2, "latitude2 is not null");
        Objects.requireNonNull(longitude2, "longitude2 is not null");
        Objects.requireNonNull(ellipsoid, "ellipsoid method is not null");
        // todo 地球坐标对象：封装经度维度坐标对象
        GlobalCoordinates source = new GlobalCoordinates(latitude, longitude);
        GlobalCoordinates target = new GlobalCoordinates(latitude2, longitude2);
        // todo 椭圆范围计算方法
        return getDistanceMeter(source, target, ellipsoid);
    }

    /**
     * 使用ellipsoidsphere方法计算距离
     */
    public static Double getDistance(Double latitude, Double longitude ,Double latitude2, Double longitude2) {
        // 椭圆范围计算方法：Ellipsoid.Sphere
        return ellipsoidMethodDistance(latitude, longitude, latitude2, longitude2, Ellipsoid.Sphere);
    }
}