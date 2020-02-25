package cn.itcast.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 生产者客户端
 */
public class KafkaProducerTest {

    public static void main(String[] args) {
        Properties props = new Properties();
        // 1.指定Kafaka集群的IP地址和端口号
        props.put("bootstrap.servers", "node-1:9092,node-2:9092,node-3:9092");
        // 2.指定等待所有副本节点的应答
        props.put("acks", "all");
        // 3.指定消息发送最大尝试次数
        props.put("retries", 0);
        // 4.指定一批消息处理大小
        props.put("batch.size", 16384);
        // 5.指定请求延时
        props.put("linger.ms", 1);
        // 6.指定缓存区内存大小
        props.put("buffer.memory", 33554432);
        // 7.设置key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 8.设置value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 9.生产数据
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 50; i++) {
            producer.send(new ProducerRecord<>("itcasttopic", Integer.toString(i), "hello world-" + i));
        }
        producer.close();
    }

}
