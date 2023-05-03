package cn.cavehicle.batch.task;

import cn.cavehicle.batch.jdbc.ReportJdbcFormat;
import cn.cavehicle.batch.utils.DateUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Types;

/**
 * 驾驶行程分析数据【离线导入】：查询每日各个行程分析指标数据，划分不同组指标，保存到MySQL数据库表中
 *      Phoenix 视图中1条数据  ->  MySQL数据库表中4条数据
 */
public class DriveTripReportAnalysisTask {

	public static void main(String[] args) throws Exception{
		// 1. 执行环境-env
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2. 数据源-source, todo: 从Phoenix视图查询数据
		DataSet<Row> reportDataSet = readFromPhoenix(env);

		// 3. 数据转换-transformation
		DataSet<Row> outputDataSet = reportDataSet.flatMap(new FlatMapFunction<Row, Row>() {
			@Override
			public void flatMap(Row value, Collector<Row> out) throws Exception {
				// 指标1：里程、SOC、行程消耗时间分析
				Row totalReportRow = getTotalReport(value);
				out.collect(totalReportRow);
				// 指标2： 高速、中速、低速次数分析
				Row speedReportRow = getSpeedReport(value);
				out.collect(speedReportRow);
				// 指标3：高速、中速、低速soc消耗分析
				Row socReportRow = getSocReport(value);
				out.collect(socReportRow);
				// 指标4：高速、中速、低速里程分析
				Row mileageReportRow = getMileageReport(value);
				out.collect(mileageReportRow);
			}

			/*
				指标：LOW_BMS_MILEAGE, MEDIUM_BMS_MILEAGE, HIGH_BMS_MILEAGE
			 */
			private Row getMileageReport(Row value) {
				// a. 获取字段值
				String vin = (String) value.getField(0);
				String tripStartTime = (String) value.getField(1);
				String lowBmsMileage = (String) value.getField(11);
				String mediumBmsMileage = (String) value.getField(12);
				String highBmsMileage = (String) value.getField(13);
				// b. 创建Row对象，设置值
				Row row = new Row(8);
				row.setField(0, DateUtil.getYesterdayDate());
				row.setField(1, vin);
				row.setField(2, 3);
				row.setField(3, "高速-中速-低速里程分析");
				row.setField(4, lowBmsMileage);
				row.setField(5, mediumBmsMileage);
				row.setField(6, highBmsMileage);
				row.setField(7, tripStartTime);

				// c. 返回Row对象
				return row;
			}

			/*
				指标：LOW_BMS_SOC, MEDIUM_BMS_SOC, HIGH_BMS_SOC
			 */
			private Row getSocReport(Row value) {
				// a. 获取字段值
				String vin = (String) value.getField(0);
				String tripStartTime = (String) value.getField(1);
				String lowBmsSoc = (String) value.getField(8);
				String mediumBmsSoc = (String) value.getField(9);
				String highBmsSoc = (String) value.getField(10);
				// b. 创建Row对象，设置值
				Row row = new Row(8);
				row.setField(0, DateUtil.getYesterdayDate());
				row.setField(1, vin);
				row.setField(2, 2);
				row.setField(3, "高速-中速-低速里程soc消耗分析");
				row.setField(4, lowBmsSoc);
				row.setField(5, mediumBmsSoc);
				row.setField(6, highBmsSoc);
				row.setField(7, tripStartTime);

				// c. 返回Row对象
				return row;
			}

			/*
				指标：TOTAL_LOW_SPEED_NUMS, TOTAL_MEDIUM_SPEED_NUMS, TOTAL_HIGH_SPEED_NUMS
			 */
			private Row getSpeedReport(Row value) {
				// a. 获取字段值
				String vin = (String) value.getField(0);
				String tripStartTime = (String) value.getField(1);
				String totalLowSpeedNums = (String) value.getField(5);
				String totalMediumSpeedNums = (String) value.getField(6);
				String totalHighSpeedNums = (String) value.getField(7);
				// b. 创建Row对象，设置值
				Row row = new Row(8);
				row.setField(0, DateUtil.getYesterdayDate());
				row.setField(1, vin);
				row.setField(2, 4);
				row.setField(3, "高速-中速-低速次数分析");
				row.setField(4, totalLowSpeedNums);
				row.setField(5, totalMediumSpeedNums);
				row.setField(6, totalHighSpeedNums);
				row.setField(7, tripStartTime);

				// c. 返回Row对象
				return row;
			}

			/*
				指标： MILEAGE, SOC_CONSUMPTION, TIME_CONSUMPTION
			 */
			private Row getTotalReport(Row value) {
				// a. 获取字段值
				String vin = (String) value.getField(0);
				String tripStartTime = (String) value.getField(1);
				String mileage = (String) value.getField(2);
				String socConsumption = (String) value.getField(3);
				String timeConsumption = (String) value.getField(4);
				// b. 创建Row对象，设置值
				Row row = new Row(8);
				row.setField(0, DateUtil.getYesterdayDate());
				row.setField(1, vin);
				row.setField(2, 1);
				row.setField(3, "里程-soc-行程消耗时间分析");
				row.setField(4, mileage);
				row.setField(5, socConsumption);
				row.setField(6, timeConsumption);
				row.setField(7, tripStartTime);

				// c. 返回Row对象
				return row;
			}
		}) ;

		// 4. 数据接收器-sink
		writeToMySQL(outputDataSet);

		// 5. 触发执行-execute
		env.execute("DriveTripReportAnalysisTask") ;
	}

	/**
	 * 从Phoenix表中加载数据，封装到DataSet数据集
	 */
	private static DataSet<Row> readFromPhoenix(ExecutionEnvironment env){
		// a. 插入SQL依据
		String querySql = "SELECT VIN, TRIP_START_TIME, " +
			"    MILEAGE, SOC_CONSUMPTION, TIME_CONSUMPTION, " +
			"    TOTAL_LOW_SPEED_NUMS, TOTAL_MEDIUM_SPEED_NUMS, TOTAL_HIGH_SPEED_NUMS, " +
			"    LOW_BMS_SOC, MEDIUM_BMS_SOC, HIGH_BMS_SOC, " +
			"    LOW_BMS_MILEAGE, MEDIUM_BMS_MILEAGE, HIGH_BMS_MILEAGE " +
			"FROM VEHICLE_NS.DRIVE_TRIP_REPORT" ;
		// b. 数据类型
		RowTypeInfo rowTypeInfo = new RowTypeInfo(
			BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
		);
		// c. 创建InputFormat对象
		JDBCInputFormat inputFormat = ReportJdbcFormat.getPhoenixJDBCInputFormat(querySql, rowTypeInfo);
		// d. 读取数据，并返回
		return env.createInput(inputFormat) ;
	}

	/**
	 * 将数据集DataSet数据，写入到MySQL数据库
	 */
	private static void writeToMySQL(DataSet<Row> dataSet) {
		// a. 插入数据库语句
		String insertSql = "INSERT INTO vehicle_networking.trip_report_result(\n" +
			"    report_date, vin, analyze_type, analyze_name, analyze_value1, analyze_value2, analyze_value3, terminal_time\n" +
			") VALUES (?, ?, ?, ?, ?, ?, ?, ?) " ;
		// b. 定义数据类型
		int[] types = new int[]{
			Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.VARCHAR,
			Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR
		};
		// c. 创建OutputFormat对象
		JDBCOutputFormat outputFormat = ReportJdbcFormat.getMySQLJDBCOutputFormat(insertSql, types);
		// d. 保存数据
		dataSet.output(outputFormat);
	}
}
