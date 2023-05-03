package cn.cavehicle.streaming.task.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 加载电子围栏表中数据，主要用于确定驶出电子围栏数据对应结果表ID，封装Map集合
 *      key
 *          vin + eleId
 *      value：
 *          Tuple2<id, inTime>
 */
public class MySQLElectricFenceSource extends RichSourceFunction<Map<String, Tuple2<Long, String>>> {
	// 定义标识符
	private boolean isRunning = true ;

	// 定义变量
	private Connection conn = null ;
	private PreparedStatement pstmt = null ;
	// 定义变量，获取Job作业全局参数
	private ParameterTool parameterTool = null ;

	@Override
	public void open(Configuration parameters) throws Exception {
		// 实例化全局参数的值
		parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
		// a. 加载驱动类
		Class.forName(parameterTool.getRequired("jdbc.driver"));
		// b. 获取连接
		conn = DriverManager.getConnection(
			parameterTool.getRequired("jdbc.url"),
			parameterTool.getRequired("jdbc.user"),
			parameterTool.getRequired("jdbc.password")
		);
		// c. 实例化Statement对象
		pstmt = conn.prepareStatement(
			"SELECT vin, ele_id, MAX(in_time) AS in_time, MAX(id) AS id\n" +
				"FROM vehicle_networking.electric_fence\n" +
				"WHERE in_time is not null  AND IFNULL(out_time, '') = ''\n" +
				"GROUP BY vin, ele_id"
		);
	}

	@Override
	public void run(SourceContext<Map<String, Tuple2<Long, String>>> ctx) throws Exception {
		while (isRunning){
			// d. 查询数据库
			ResultSet resultSet = pstmt.executeQuery();
			// e. 获取数据
			Map<String, Tuple2<Long, String>> map = new HashMap<>() ;
			while (resultSet.next()){
				// 主键Key = vin + eleId
				String key = resultSet.getString("vin") + "_" + resultSet.getInt("ele_id");
				// 值Value = Tuple2<id, inTime>
				Tuple2<Long, String> value = Tuple2.of(
					resultSet.getLong("id"), resultSet.getString("in_time")
				);
				// 放入map集合
				map.put(key, value) ;
			}
			// f. 输出数据
			ctx.collect(map);
			// 关闭连接，释放资源
			if(!resultSet.isClosed()) resultSet.close();

			// 设置时间间隔，每隔多久查询数据库
			TimeUnit.MILLISECONDS.sleep(parameterTool.getLong("electric.fence.millionseconds", 6000));
		}
	}

	@Override
	public void cancel() {
		isRunning = false ;
	}

	// 清理工作，比如释放连接
	@Override
	public void close() throws Exception {
		if(null != pstmt) pstmt.close();
		if(null != conn) conn.close();
	}
}
