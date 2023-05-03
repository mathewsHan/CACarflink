package cn.cavehicle.flink.connector;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 模拟实时产生用户访问网站日志数据
 */
public class MockUserTrackLog {

	public static void main(String[] args) throws Exception{
		// 1. 创建KafkaProducer对象
		Properties props = new Properties();
		props.put("bootstrap.servers", "node1.itcast.cn:9092,node2.itcast.cn:9092,node3.itcast.cn:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("acks", "1") ;
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props) ;

		// 2. 模拟产生数据
		String[] types = new String[]{
			"click", "browser", "search", "click", "browser", "browser", "browser",
			"click", "search", "click", "browser", "click", "browser", "browser", "browser"
		} ;
		Random random = new Random() ;
		FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS") ;
		while (true){
			String userId = "user_" + (random.nextInt(1000) + 1000);
			int itemId = 10000 + random.nextInt(10000) ;
			String behavior = types[random.nextInt(types.length)];
			String ts = format.format(System.currentTimeMillis()) ;
			String logEvent = userId + "," + itemId + ","  + behavior + "," + ts ;
			System.out.println(logEvent);

			// 3. 发送数据至Topic
			ProducerRecord<String, String> record = new ProducerRecord<>("log-topic", logEvent) ;
			kafkaProducer.send(record) ;

			// 每隔1秒产生1条数据
			TimeUnit.MILLISECONDS.sleep(100);
		}
	}

}