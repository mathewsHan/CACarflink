package cn.cavehicle.streaming.sink;

import cn.cavehicle.utils.ConfigLoader;
import cn.cavehicle.utils.DateUtil;
import cn.cavehicle.entity.VehicleDataObj;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Timer;
import java.util.TimerTask;

/**
 * 自定义Sink，通过Phoenix JDBC将数据写入Phoenix表中，并且实现批量插入和定时刷新
 *      todo: 1) 批量写入，加上事务  2) 时间间隔flush刷新写入
 */
public class SrcDetailDataToPhoenixSink extends RichSinkFunction<VehicleDataObj> {
	//  定义变量
	private Connection connection = null ;
	private PreparedStatement pstmt = null ;

	// 定义变量：计数器
	private Integer counter = 0 ;
	// 定义变量：最新数据加入批次时间
	private Long addBatchTime = System.currentTimeMillis();

	@Override
	public void open(Configuration parameters) throws Exception {
		// a. 加载驱动类
		Class.forName("org.apache.phoenix.jdbc.PhoenixDriver") ;
		// b. 获取连接
		String jdbcUrl = "jdbc:phoenix:" + ConfigLoader.get("zookeeper.quorum") + ":" + ConfigLoader.get("zookeeper.clientPort");
		connection = DriverManager.getConnection(jdbcUrl) ;
		connection.setAutoCommit(false) ; // 设置手动提交
		// c. 创建Statement对象
		pstmt = connection.prepareStatement("UPSERT INTO VEHICLE_SRC_DETAIL_JDBC VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)") ;

		// todo: 定时任务，每隔10秒，触发一次，将批次中数据批量写入
		new Timer().schedule(
			new TimerTask() {
				@Override
				public void run() {
					try{
						// 计算批次中最新加入数据时间与当前时间间隔
						Long interval = System.currentTimeMillis() - addBatchTime ;
						// 判断是否有数据
						if(interval >= 5000L && counter > 0){
							System.out.println("定时任务，触发一次批量插入数据：" + counter + "................");
							pstmt.executeBatch();
							connection.commit();
							counter = 0 ;
						}
					}catch (Exception e){
						e.printStackTrace();
					}
				}
			},5000L, 10000L
		);
	}

	@Override
	public void invoke(VehicleDataObj vehicleDataObj, Context context) throws Exception {
		// d. 设置占位置
		// 获取RowKey
		String vin = vehicleDataObj.getVin();
		String terminalTime = vehicleDataObj.getTerminalTime();
		String rowKey = StringUtils.reverse(vin) + "_" + terminalTime;
		// 依据索引设置值
		pstmt.setString(1, rowKey);
		pstmt.setString(2, vin);
		pstmt.setString(3, terminalTime);
		pstmt.setString(4, vehicleDataObj.getCurrentElectricity() + "");
		pstmt.setString(5, vehicleDataObj.getRemainPower() + "");
		pstmt.setString(6, vehicleDataObj.getFuelConsumption100km());
		pstmt.setString(7, vehicleDataObj.getEngineSpeed());
		pstmt.setString(8, vehicleDataObj.getSpeed() + "");
		pstmt.setString(9, DateUtil.getCurrentDateTime());
		// e. 执行插入
		// pstmt.execute() ;

		// todo: 加入批次
		pstmt.addBatch();
		counter ++ ;
		addBatchTime = System.currentTimeMillis() ;

		// 如果批次中数据量大于5000条时，批量写入一次
		if(counter >= 5000){
			System.out.println("数据量达到大于等于5000，触发一次批量插入数据：" + counter + "................");
			pstmt.executeBatch();
			connection.commit();
			// 重置计数器
			counter = 0 ;
		}
	}

	@Override
	public void close() throws Exception {
		// 关闭之前，批量插入
		pstmt.executeBatch();
		connection.commit();

		// f. 关闭连接，释放资源
		if(null != pstmt) pstmt.close();
		if(null != connection) connection.close();
	}


}
