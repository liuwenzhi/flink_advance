package cn.supcon.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * kafka源端样例，从kafka源端topic读取数据，直接打印输出
 */
public class KafkaSourceDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 构造kafka源端对象，包括连接方式，消费者组，topic信息，反序列化，消费offset信息
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder().setBootstrapServers("172.16.2.62:9092").setGroupId("supcon").setTopics("topic_test")
                .setValueOnlyDeserializer(new SimpleStringSchema()).setStartingOffsets(OffsetsInitializer.earliest()).build();
        // 从kafka源端读取数据，直接输出
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"kafkaSource").print();
        env.execute();
    }
}
