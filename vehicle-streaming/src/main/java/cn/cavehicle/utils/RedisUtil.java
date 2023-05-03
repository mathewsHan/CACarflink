package cn.cavehicle.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 编写工具类，操作Redis数据库： 获取Jedis连接，基于连接池获取连接
 *      set(key, value) 设置数据
 *      get(key) 获取数据
 */
public class RedisUtil {

	// 定义变量
	private static JedisPool jedisPool ;

	// 静态代码块，进行初始化
	static {
		// 获取属性配置
		JedisPoolConfig config = new JedisPoolConfig();
		// 实例化对象
		jedisPool = new JedisPool(config, ConfigLoader.get("redis.host"), ConfigLoader.getInt("redis.port"));
	}

	// 获取Jedis实例对象
	private static Jedis getJedis(){
		return jedisPool.getResource();
	}

	/**
	 * 设置数据： set key value
	 */
	public static void setValue(String key, String value){
		// a. 获取Jedis连接
		Jedis jedis = getJedis() ;
		// b. 设置数据
		jedis.set(key.getBytes(), value.getBytes()) ;
		// c. 释放资源
		jedis.close();
	}

	/**
	 *
	 */
	public static String getValue(String key){
		// a. 获取Jedis连接
		Jedis jedis = getJedis() ;
		// b. 获取数据
		byte[] value = jedis.get(key.getBytes());
		String returnValue = null ;
		if(null != value){
			returnValue = new String(value) ;
		}
		// c. 释放资源
		jedis.close();
		// d. 返回获取字符串
		return returnValue ;
	}

}
