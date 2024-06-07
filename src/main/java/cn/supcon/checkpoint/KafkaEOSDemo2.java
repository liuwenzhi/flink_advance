package cn.supcon.checkpoint;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.IsolationLevel;

import java.time.Duration;

/**
 * flink去消费 被 两阶段提交的 topic，设置隔离级别
 * 本例主要是验证下两阶段提交，差不多每5秒钟收到一次数据，预提交阶段的数据，不会收到
 * EOS：精准一次
 *
 * <p>
 * Flink 1.17 130课
 */
public class KafkaEOSDemo2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 消费 在前面使用两阶段提交写入的Topic
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("node01:9092,node02:9092,node03:9092")
                .setGroupId("atguigu")
                .setTopics("test") // 消费被两阶段提交的topic数据
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                // TODO 作为 下游的消费者，要设置 事务的隔离级别 = 读已提交
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .build();
        env
                .fromSource(kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)), "kafkasource")
                .print();

        env.execute();
    }
}
