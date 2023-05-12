package cn.cavehicle.KafkaExactlyOnceSematic;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.Random;

/*
    自定义Kafka的分区器

 */
public class UserPartition implements Partitioner {


    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        Integer parts = cluster.partitionCountForTopic(topic);
        Random random = new Random();
        int part = random.nextInt();
        return part;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
