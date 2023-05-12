package cn.cavehicle.demo;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public class KafkaProducerTest {


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        KafkaProducer<String,String> producer  = new KafkaProducer<>(props);


    }
}
