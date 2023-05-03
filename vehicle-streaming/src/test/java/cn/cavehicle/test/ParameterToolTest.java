package cn.cavehicle.test;

import org.apache.flink.api.java.utils.ParameterTool;

public class ParameterToolTest {

	public static void main(String[] args) throws Exception{
		// 参数解析工具类
		/*
			要求args参数类型：
				--input /datas/words.txt --output /datas/wc-output.txt
		 */
		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		System.out.println(parameterTool.get("input"));
		System.out.println(parameterTool.get("output"));

		/*
			属性配置文件
		 */
		ParameterTool tool = ParameterTool.fromPropertiesFile("D:\\Vehicle_Project\\vehicle-parent\\vehicle-streaming\\src\\main\\resources\\config.properties") ;
		System.out.println(tool.get("hdfsUri"));
	}

}
