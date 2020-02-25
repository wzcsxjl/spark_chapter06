package cn.itcast.kafka.streams;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.HashMap;

public class LogProcessor implements Processor<byte[], byte[]> {

    private ProcessorContext processorContext;

    /**
     * 初始化上下文对象
     *
     * @param processorContext
     */
    @Override
    public void init(ProcessorContext processorContext) {
        this.processorContext = processorContext;
    }

    /**
     * 每接收到一条消息时，都会调用该方法处理并更新状态进行存储
     *
     * @param key
     * @param value
     */
    @Override
    public void process(byte[] key, byte[] value) {
        String inputOri = new String(value);
        HashMap<String, Integer> map = new HashMap<>();
        int times = 1;
        if (inputOri.contains(" ")) {
            // 截取字段
            String[] words = inputOri.split(" ");
            for (String word : words) {
                if (map.containsKey(word)) {
                    map.put(word, map.get(word) + 1);
                } else {
                    map.put(word, times);
                }
            }
        }
        inputOri = map.toString();
        processorContext.forward(key, inputOri.getBytes());
    }

    /**
     * 关闭处理器，在这里可以做一些资源清理工作
     */
    @Override
    public void close() {

    }

}
