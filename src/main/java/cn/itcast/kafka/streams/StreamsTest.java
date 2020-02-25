package cn.itcast.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.HashMap;
import java.util.Map;

public class StreamsTest {

    public static void main(String[] args) {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "node-1:9092,node-2:9092,node-3:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("my-input-topic")
                .mapValues(value -> value.toString())
                .to("my-output-topic");
        KafkaStreams streams = new KafkaStreams(builder.build(), (StreamsConfig) props);
        streams.start();
    }

}
