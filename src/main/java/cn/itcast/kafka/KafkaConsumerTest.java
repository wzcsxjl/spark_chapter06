package cn.itcast.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * 消费者客户端
 */
public class KafkaConsumerTest {

    public static void main(String[] args) {
        // 1.准备配置文件
        Properties props = new Properties();
        // 2.指定Kafka集群主机名和端口号
        props.put("bootstrap.servers", "node-1:9092,node-2:9092,node-3:9092");
        // 3.指定消费者组id，在同一时刻同一消费组中只有一个线程可以去消费一个分区消息，不同的消费组可以去消费同一个分区的消息
        props.put("group.id", "itcasttopic");
        // 4.自动提交偏移量
        props.put("enable.auto.commit", "true");
        // 5.自动提交时间间隔，每秒提交一次
        // 每秒向Zookeeper中写入每个分区的偏移量
        props.put("auto.commit.interval.ms", "1000");
        // 将消息数据进行反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        // 6.订阅消息，这里的topic可以是多个
        kafkaConsumer.subscribe(Arrays.asList("itcasttopic"));
        // 7.获取消息
        while (true) {
            // 每隔100ms就拉取一次
            // ConsumerRecords对象是一个容器，用于保存特定主题的每个分区列表
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic = %s, offset = %d, key = %s, value = %s%n",
                        record.topic(), record.offset(), record.key(), record.value());
            }
        }
    }

}
