package cn.cavehicle.batch.task;

import cn.cavehicle.batch.jdbc.AccuracyJdbcFormat;
import cn.cavehicle.batch.utils.DateUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Types;

/**
 * 离线指标统计：总的数据准确率计算 -> 从Hive表加载数据，向MySQL数据库写入数据
 */
public class VehicleDataAccuracyTask {

	public static void main(String[] args) throws Exception {
		// 1、获取执行环境-env
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2、数据源-source, todo: JDBCInputFormat对象
		/*
			Hive查询SQL语句
			数据类型
			获取InputFormat实例
		 */
		String querySql = "SELECT srcTotalNum, errorTotalNum FROM " +
			"(SELECT COUNT(1) AS srcTotalNum FROM vehicle_ods.vehicle_src) src, " +
			"(SELECT COUNT(1) AS errorTotalNum FROM vehicle_ods.vehicle_error) error" ;
		RowTypeInfo rowTypeInfo = new RowTypeInfo(
			BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO
		) ;
		JDBCInputFormat hiveInputFormat = AccuracyJdbcFormat.getHiveJDBCInputFormat(querySql, rowTypeInfo);
		DataSource<Row> totalDataSet = env.createInput(hiveInputFormat);
		// totalDataSet.printToErr();

		// 3、数据转换-transformation
		/*
			返回结果：
				98318, 1516
			计算准确率和错误率
		 */
		MapOperator<Row, Row> rateDataSet = totalDataSet.map(new MapFunction<Row, Row>() {
			@Override
			public Row map(Row value) throws Exception {
				// a. 获取正确条目数和错误条目数
				Long srcTotalNum = (Long) value.getField(0);
				Long errorTotalNum = (Long) value.getField(1);
				// b.  计算准确率和错误率
				double accuracy = srcTotalNum / ((srcTotalNum + errorTotalNum) * 1.0) ;
				double errorRate = 1 - accuracy ;
				// c. 封装到Row对象
				Row outputRow = new Row(6) ;
				outputRow.setField(0, DateUtil.getYesterdayDate());
				outputRow.setField(1, "all");
				outputRow.setField(2, srcTotalNum);
				outputRow.setField(3, errorTotalNum);
				outputRow.setField(4, accuracy);
				outputRow.setField(5, errorRate);
				// d. 返回值
				return outputRow;
			}
		});

		// 4、数据接收器-sink，todo：JDBCOutputFormat对象
		/*
		 	MySQL插入SQL语句
		 	数据类型
			获取OutputFormat实例
		 */
		String insertSql = "INSERT INTO vehicle_networking.vehicle_data_rate( " +
			"   rate_date, rate_type, src_total_num, error_total_num, data_accuracy, data_error_rate " +
			") VALUES (?, ?, ?, ?, ?, ?)" ;
		int[] typesArray = new int[]{
			Types.VARCHAR, Types.VARCHAR, Types.BIGINT, Types.BIGINT, Types.FLOAT, Types.FLOAT
		} ;
		JDBCOutputFormat mysqlOutputFormat = AccuracyJdbcFormat.getMySQLJDBCOutputFormat(insertSql, typesArray);
		rateDataSet.output(mysqlOutputFormat) ;

		// 5、触发执行-execute
		env.execute("VehicleDataAccuracyTask");
	}

}
