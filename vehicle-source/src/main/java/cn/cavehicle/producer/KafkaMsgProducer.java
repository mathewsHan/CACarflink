package cn.cavehicle.producer;

import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * 模拟发送数据：直接调用Kafka Producer API 写入数据至Topic队列中
 */
public class KafkaMsgProducer {

	/**
	 * 定义类，实现线程Runnable接口，运行run 方法，读取数据和发送数据
	 */
	private static class MsgProducer implements Runnable{

		// 从文件中一行一行读取数据，发送到KafkaTopic中
		@SneakyThrows
		@Override
		public void run() {
			// 1. 创建KafkaProducer对象
			Properties props = new Properties();
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1.itcast.cn:9092,node2.itcast.cn:9092,node3.itcast.cn:9092");
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
			props.put(ProducerConfig.ACKS_CONFIG, "1") ;
			KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props) ;

			// 声明字符缓冲流读取数据
			BufferedReader bufferedReader = null ;
			try{
				// 2. 从文件中一行一行读取数据
				File file = new File("F:\\狂野大数据3期-车联网项目\\vehicle_day12_20220602\\03_代码及数据\\vehicle-parent\\datas\\sourcedata.txt") ;
				bufferedReader = new BufferedReader(
					new InputStreamReader(new FileInputStream(file))
				);

				String line;
				int counter = 1;
				while ((line = bufferedReader.readLine()) != null){
					// ProducerRecord(Topic,Value) 所有数据写入一个分区.. ProduceRecord(Topic,Key,Value)- 按照Hash取余

					ProducerRecord<String, String> record = new ProducerRecord<>("vehicle-data", line) ;
					kafkaProducer.send(record) ;

					System.out.println("模拟数据发送程序，消息生产生产第" + counter + "条数据......");
					counter ++ ;
				}
			}catch (Exception e){
				e.printStackTrace();
			}finally {
				if(null != bufferedReader) bufferedReader.close();
				// 关闭资源
				kafkaProducer.close();
			}
		}
	}


	public static void main(String[] args) {
		// 创建线程，并启动
		new Thread(new MsgProducer()).start();
	}

}
