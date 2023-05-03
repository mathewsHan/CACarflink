package cn.cavehicle.streaming.sink;

import cn.cavehicle.utils.DateUtil;
import cn.cavehicle.entity.VehicleDataObj;
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
 * 自定义Sink实现：将正确数据实时写入HBase数据库表中，后期集成Phoenix进行即席查询
 */
public class SrcDetailDataToHBaseSink extends RichSinkFunction<VehicleDataObj> {

	// 定义变量，表名称，外部传递
	private String tableName ;

	// 使用构造方法，接收表的名称
	public SrcDetailDataToHBaseSink(String tableName){
		this.tableName = tableName ;
	}

	// 定义一个常量
	private static final byte[] cfBytes= Bytes.toBytes("INFO") ;

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

	// 流中每条数据处理
	@Override
	public void invoke(VehicleDataObj vehicleDataObj, Context context) throws Exception {
		/*
			RowKey: reverse(vin) + _ + terminalTime
		 */
		// d. 创建Put对象
		Put put = createPut(vehicleDataObj) ;
		// e. 将put对象数据插入到表中
		bufferedMutator.mutate(put);
	}

	/**
	 * 将数据流中每条数据，封装到Put对象中，todo：向HBase表中写入数据时，所有值都是byte[] 字节数组，使用工具类Bytes
	 */
	private Put createPut(VehicleDataObj vehicleDataObj) {
		// step1、RowKey创建
		String rowKey = StringUtils.reverse(vehicleDataObj.getVin()) + "_" + vehicleDataObj.getTerminalTime();

		// step2、构建Put对象
		Put put = new Put(Bytes.toBytes(rowKey)) ;

		// step3、添加列
		put.addColumn(cfBytes, Bytes.toBytes("vin"), Bytes.toBytes(vehicleDataObj.getVin()));
		put.addColumn(cfBytes, Bytes.toBytes("terminalTime"), Bytes.toBytes(vehicleDataObj.getTerminalTime())) ;
		// todo: 额外额外一个字段，表示当前数据处理时间
		put.addColumn(cfBytes, Bytes.toBytes("processTime"), Bytes.toBytes(DateUtil.getCurrentDateTime())) ;

		if (vehicleDataObj.getCurrentElectricity() != -999999) put.addColumn(cfBytes, Bytes.toBytes("currentElectricity"), Bytes.toBytes(String.valueOf(vehicleDataObj.getCurrentElectricity())));
		if (vehicleDataObj.getRemainPower() != -999999) put.addColumn(cfBytes, Bytes.toBytes("remainPower"), Bytes.toBytes(String.valueOf(vehicleDataObj.getRemainPower())));

		if (!vehicleDataObj.getFuelConsumption100km().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("fuelConsumption100km"), Bytes.toBytes(vehicleDataObj.getFuelConsumption100km()));
		if (!vehicleDataObj.getEngineSpeed().isEmpty()) put.addColumn(cfBytes, Bytes.toBytes("engineSpeed"), Bytes.toBytes(vehicleDataObj.getEngineSpeed()));
		if (vehicleDataObj.getVehicleSpeed() != -999999) put.addColumn(cfBytes, Bytes.toBytes("vehicleSpeed"), Bytes.toBytes(String.valueOf(vehicleDataObj.getVehicleSpeed())));

		// 返回Put对象
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
