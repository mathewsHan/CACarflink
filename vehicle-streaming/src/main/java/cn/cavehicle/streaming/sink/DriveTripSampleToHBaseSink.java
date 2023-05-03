package cn.cavehicle.streaming.sink;

import cn.cavehicle.utils.DateUtil;
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
 * 自定义Sink实现：将驾驶行程采样数据实时写入到HBase表中，后续与Phoenix集成，使用SQL查询行程采样数据
 */
public class DriveTripSampleToHBaseSink extends RichSinkFunction<String[]> {

	// 定义变量，表名称，外部传递
	private String tableName ;

	public DriveTripSampleToHBaseSink(String tableName){
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
	public void invoke(String[] value, Context context) throws Exception {
		/*
			RowKey: reverse(vin) + _ + terminalTime
		 */
		// d. 创建Put对象
		Put put = createPut(value) ;
		// e. 将put对象数据插入到表中
		bufferedMutator.mutate(put);
	}

	/**
	 * 将每个行程采样数据封装到Put对象中
	 */
	private Put createPut(String[] values) {
		// a. RowKey 主键
		String rowKey = StringUtils.reverse(values[0]) + "_" + values[1] ;
		// b. Put 对象
		Put put = new Put(Bytes.toBytes(rowKey)) ;
		// c. 添加列
		byte[] cfBytes= Bytes.toBytes("INFO") ;
		put.addColumn(cfBytes, Bytes.toBytes("VIN"), Bytes.toBytes(values[0])) ;
		put.addColumn(cfBytes, Bytes.toBytes("TERMINAL_TIME"), Bytes.toBytes(values[2])) ;
		put.addColumn(cfBytes, Bytes.toBytes("SOC"), Bytes.toBytes(values[3])) ;
		put.addColumn(cfBytes, Bytes.toBytes("MILEAGE"), Bytes.toBytes(values[4])) ;
		put.addColumn(cfBytes, Bytes.toBytes("SPEED"), Bytes.toBytes(values[5])) ;
		put.addColumn(cfBytes, Bytes.toBytes("GPS"), Bytes.toBytes(values[6])) ;
		put.addColumn(cfBytes, Bytes.toBytes("PROCESS_TIME"), Bytes.toBytes(DateUtil.getCurrentDateTime())) ;
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
