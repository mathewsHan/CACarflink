package cn.cavehicle.flink.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * 基于Flink SQL实现加载文本文件数据，进行电影评分统计：Top10电影分析
 */
public class FlinkSQLDemo {

	public static void main(String[] args) {
		// 1. 构建表执行环境
		EnvironmentSettings settings = EnvironmentSettings
			.newInstance()
			.inBatchMode() // 设置批模式处理数据
			.useBlinkPlanner() // 底层引擎：Blink，默认引擎
			.build() ;
		TableEnvironment tableEnv = TableEnvironment.create(settings) ;

		// 2. 创建输入表：编写CREATE TABLE依据，映射到数据（本地文件系统文件）
		tableEnv.executeSql(
			"CREATE TABLE tbl_ratings(\n" +
				"  user_id STRING,\n" +
				"  movie_id STRING,\n" +
				"  rating DOUBLE,\n" +
				"  ts BIGINT\n" +
				") WITH (\n" +
				"  'connector' = 'filesystem', \n" +
				"  'path' = 'datas/ratings.data', \n" +
				"  'format' = 'csv',\n" +
				"  'csv.field-delimiter' = '\\t',\n" +
				"  'csv.ignore-parse-errors' = 'true'\n" +
				")"
		);

		// 3. 查询表数据，编写SQL语句
		/*
			每个电影平均评分，评分次数
		 */
		TableResult tableResult = tableEnv.executeSql(
			"WITH tmp AS(" +
					"SELECT " +
					"   movie_id, COUNT(movie_id) AS rating_people, ROUND(AVG(rating), 2) AS rating_number " +
					"FROM " +
					"   tbl_ratings GROUP BY movie_id " +
				")" +
				"SELECT * FROM tmp WHERE rating_people > 400 ORDER BY rating_number DESC, rating_people DESC LIMIT 20"
		);
		tableResult.print();
	}

}
