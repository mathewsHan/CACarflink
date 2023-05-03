package cn.cavehicle.test.utils;

import cn.cavehicle.utils.ConfigLoader;

public class ConfigLoaderTest {

	public static void main(String[] args) {

		System.out.println(ConfigLoader.get("jdbc.user"));
		System.out.println(ConfigLoader.get("jdbc.ssl"));

		System.out.println("=========================");

		System.out.println(ConfigLoader.getInt("zookeeper.clientPort"));
		System.out.println(ConfigLoader.getInt("hbase.clientPort"));

	}

}
