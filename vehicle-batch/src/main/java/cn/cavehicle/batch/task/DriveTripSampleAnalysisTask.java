package cn.cavehicle.batch.task;

import cn.cavehicle.batch.jdbc.ReportJdbcFormat;
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
 * 驾驶行程采样数据【离线统计】：查询每日驾驶行程样本总条目数，保存到MySQL数据库表中
 */
public class DriveTripSampleAnalysisTask {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2. 数据源-source
		JDBCInputFormat inputFormat = ReportJdbcFormat.getPhoenixJDBCInputFormat(
			//"SELECT COUNT(1) AS TOTAL FROM VEHICLE_NS.DRIVE_TRIP_SAMPLE WHERE PROCESS_TIME like '2022-05-26%'",
			"SELECT COUNT(1) AS TOTAL FROM VEHICLE_NS.DRIVE_TRIP_SAMPLE",
			new RowTypeInfo(BasicTypeInfo.LONG_TYPE_INFO)
		);
		DataSource<Row> reportDataSet = env.createInput(inputFormat);

		// 3. 数据转换-transformation
		MapOperator<Row, Row> outputDataSet = reportDataSet.map(new MapFunction<Row, Row>() {
			@Override
			public Row map(Row value) throws Exception {
				// a. 获取条目数
				Long totalNum = (Long) value.getField(0);
				// b. 构建Row对象
				Row row = new Row(3) ;
				row.setField(0, DateUtil.getYesterdayDate());
				row.setField(1, "day");
				row.setField(2, totalNum);
				// c. 返回
				return row;
			}
		});

		// 4. 数据接收器-sink
		JDBCOutputFormat outputFormat = ReportJdbcFormat.getMySQLJDBCOutputFormat(
			"INSERT INTO vehicle_networking.trip_sample_result(" +
				"report_date, report_type, total_number" +
				") VALUES (?, ?, ?)",
			new int[]{
				Types.VARCHAR, Types.VARCHAR, Types.BIGINT
			}
		);
		outputDataSet.output(outputFormat);

		// 5. 触发执行-execute
		env.execute("DriveTripSampleAnalysisTask") ;
	}

}
