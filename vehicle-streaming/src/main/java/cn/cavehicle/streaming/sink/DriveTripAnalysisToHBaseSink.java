package cn.cavehicle.streaming.sink;

import cn.cavehicle.utils.DateUtil;
import cn.cavehicle.entity.DriveTripReport;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 自定义Sink实现：将驾驶行程分析数据实时写入到HBase表中，后续与Phoenix集成，使用SQL查询行程分析指标数据
 */
public class DriveTripAnalysisToHBaseSink extends RichSinkFunction<DriveTripReport> {

	// 定义变量，表名称，外部传递
	private String tableName ;

	public DriveTripAnalysisToHBaseSink(String tableName){
		this.tableName = tableName;
	}

	// 定义变量
	private Connection connection = null ;
	private BufferedMutator bufferedMutator = null ;

	// 准备工作，比如创建连接
	@Override
	public void open(Configuration parameters) throws Exception {
		// todo: 获取job全局参数
		ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
		String zkQuorum = parameterTool.getRequired("zookeeper.quorum") ;
		String zkPort = parameterTool.getRequired("zookeeper.clientPort") ;

		// a. 创建配置对象，设置ZK集群地址
		org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
		configuration.set(HConstants.CLIENT_ZOOKEEPER_QUORUM, zkQuorum);
		configuration.set(HConstants.CLIENT_ZOOKEEPER_CLIENT_PORT, zkPort);

		// b. 初始化Connection对象
		connection = ConnectionFactory.createConnection(configuration) ;

		// c. 实例化Table对象
		BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName)) ;
		params.writeBufferSize(5 * 1024 * 1024) ; // 设置缓冲区大小
		params.setWriteBufferPeriodicFlushTimeoutMs(5 * 1000L) ;  // 周期性刷新缓存数据到表中
		bufferedMutator = connection.getBufferedMutator(params) ;
	}


	@Override
	public void invoke(DriveTripReport driveTripReport, Context context) throws Exception {
		/*
			RowKey: reverse(vin) + _ + terminalTime
		 */
		// d. 创建Put对象
		Put put = createPut(driveTripReport) ;
		// e. 将put对象数据插入到表中
		bufferedMutator.mutate(put);
	}

	/**
	 * 将每个行程采样数据封装到Put对象中
	 */
	private Put createPut(DriveTripReport driveTripReport) {
		// a. RowKey 主键
		String rowKey = StringUtils.reverse(driveTripReport.getVin()) + "_" + driveTripReport.getTripStartTime() ;
		// b. Put 对象
		Put put = new Put(Bytes.toBytes(rowKey)) ;
		// c. 添加列
		byte[] cfBytes= Bytes.toBytes("INFO") ;
		put.addColumn(cfBytes, Bytes.toBytes("VIN"), Bytes.toBytes(driveTripReport.getVin()));
		put.addColumn(cfBytes, Bytes.toBytes("TRIP_START_TIME"), Bytes.toBytes(driveTripReport.getTripStartTime()));
		put.addColumn(cfBytes, Bytes.toBytes("START_BMS_SOC"), Bytes.toBytes(String.valueOf(driveTripReport.getStartBmsSoc())));
		put.addColumn(cfBytes, Bytes.toBytes("START_LONGITUDE"), Bytes.toBytes(String.valueOf(driveTripReport.getStartLongitude())));
		put.addColumn(cfBytes, Bytes.toBytes("START_LATITUDE"), Bytes.toBytes(String.valueOf(driveTripReport.getStartLatitude())));
		put.addColumn(cfBytes, Bytes.toBytes("START_MILEAGE"), Bytes.toBytes(String.valueOf(driveTripReport.getStartMileage())));
		put.addColumn(cfBytes, Bytes.toBytes("END_BMS_SOC"), Bytes.toBytes(String.valueOf(driveTripReport.getEndBmsSoc())));
		put.addColumn(cfBytes, Bytes.toBytes("END_LONGITUDE"), Bytes.toBytes(String.valueOf(driveTripReport.getEndLongitude())));
		put.addColumn(cfBytes, Bytes.toBytes("END_LATITUDE"), Bytes.toBytes(String.valueOf(driveTripReport.getEndLatitude())));
		put.addColumn(cfBytes, Bytes.toBytes("END_MILEAGE"), Bytes.toBytes(String.valueOf(driveTripReport.getEndMileage())));
		put.addColumn(cfBytes, Bytes.toBytes("TRIP_END_TIME"), Bytes.toBytes(driveTripReport.getTripEndTime()));
		put.addColumn(cfBytes, Bytes.toBytes("MILEAGE"), Bytes.toBytes(String.valueOf(driveTripReport.getMileage())));
		put.addColumn(cfBytes, Bytes.toBytes("MAX_SPEED"), Bytes.toBytes(String.valueOf(driveTripReport.getMaxSpeed())));
		put.addColumn(cfBytes, Bytes.toBytes("SOC_CONSUMPTION"), Bytes.toBytes(String.valueOf(driveTripReport.getSocConsumption())));
		put.addColumn(cfBytes, Bytes.toBytes("TIME_CONSUMPTION"), Bytes.toBytes(String.valueOf(driveTripReport.getTimeConsumption())));
		put.addColumn(cfBytes, Bytes.toBytes("TOTAL_LOW_SPEED_NUMS"), Bytes.toBytes(String.valueOf(driveTripReport.getTotalLowSpeedNums())));
		put.addColumn(cfBytes, Bytes.toBytes("TOTAL_MEDIUM_SPEED_NUMS"), Bytes.toBytes(String.valueOf(driveTripReport.getTotalMediumSpeedNums())));
		put.addColumn(cfBytes, Bytes.toBytes("TOTAL_HIGH_SPEED_NUMS"), Bytes.toBytes(String.valueOf(driveTripReport.getTotalHighSpeedNums())));
		put.addColumn(cfBytes, Bytes.toBytes("LOW_BMS_SOC"), Bytes.toBytes(String.valueOf(driveTripReport.getLowBmsSoc())));
		put.addColumn(cfBytes, Bytes.toBytes("MEDIUM_BMS_SOC"), Bytes.toBytes(String.valueOf(driveTripReport.getMediumBmsSoc())));
		put.addColumn(cfBytes, Bytes.toBytes("HIGH_BMS_SOC"), Bytes.toBytes(String.valueOf(driveTripReport.getHighBmsSoc())));
		put.addColumn(cfBytes, Bytes.toBytes("LOW_BMS_MILEAGE"), Bytes.toBytes(String.valueOf(driveTripReport.getLowBmsMileage())));
		put.addColumn(cfBytes, Bytes.toBytes("MEDIUM_BMS_MILEAGE"), Bytes.toBytes(String.valueOf(driveTripReport.getMediumBmsMileage())));
		put.addColumn(cfBytes, Bytes.toBytes("HIGH_BMS_MILEAGE"), Bytes.toBytes(String.valueOf(driveTripReport.getHighBmsMileage())));
		put.addColumn(cfBytes, Bytes.toBytes("TRIP_STATUS"), Bytes.toBytes(String.valueOf(driveTripReport.getTripStatus())));
		put.addColumn(cfBytes, Bytes.toBytes("PROCESS_TIME"), Bytes.toBytes(DateUtil.getCurrentDateTime()));
		// d. 返回对象
		return put;
	}

	// 收尾工作，比如关闭连接
	@Override
	public void close() throws Exception {
		// f. 关闭连接
		if(null != bufferedMutator) bufferedMutator.close();
		if(null != connection) connection.close();
	}
}
