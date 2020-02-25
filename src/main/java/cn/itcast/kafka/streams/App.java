package cn.itcast.kafka.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.util.Properties;

public class App {

    public static void main(String[] args) {
        // 声明来源主题
        String fromTopic = "testStreams1";
        // 声明目标主题
        String toTopic = "testStreams2";
        // 设置KafkaStreams参数信息
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "logProcessor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "node-1:9092,node-2:9092,node-3:9092");
        // 实例化StreamsConfig对象
        StreamsConfig config = new StreamsConfig(props);
        // 构建拓扑结构
        Topology topology = new Topology();
        // 添加源处理节点，为源处理节点指定名称和它订阅的主题
        topology.addSource("SOURCE", fromTopic)
                // 添加自定义处理节点，指定处理器类和上一节点的名称
                .addProcessor("PROCESSOR", new ProcessorSupplier() {
                    @Override
                    public Processor get() {
                        return new LogProcessor();
                    }
                }, "SOURCE")
                // 添加目标处理节点，需要指定目标处理节点和上一节点的名称
                .addSink("SINK", toTopic, "PROCESSOR");
        // 实例化KafkaStreams对象
        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.start();
    }

}
