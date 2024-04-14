package cn.supcon.source2;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从kafka源端读取数据
 * <p>
 * kafka消费参数知识点补充：auto.reset.offsets
 * earliest:如果有offset，从offset继续消费；如果没有offset，从最早消费
 * latest:如果有offset，从offset继续消费；如果没有offset，从最新消费
 * <p>
 * flink的kafkasource，offset消费策略：OffsetsInitializer，这里和kafka的消费策略有区别
 * earliest：一定从最早开始消费 （OffsetsInitializer默认这个策略）
 * latest：一定从最新开始消费
 * Flink1.17课程36课
 */
public class KafkaSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 创建kafka的源，直接在builder前边加上泛型，指定读入数据类型
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder().setBootstrapServers("node01:9092,node02:9092,node03:9092")
                .setGroupId("group1")       // 设置flink消费者组id，这个可以随意指定一个标识名称
                .setTopics("test_20240414")
                .setValueOnlyDeserializer(new SimpleStringSchema())  // 设置反序列化，kafka是源端，flink是消费端，数据通过序列化从kafka走到flink，flink端需要反序列化，这里只对value做反序列化
                .setStartingOffsets(OffsetsInitializer.latest()).build();
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkasource").print();
        env.execute();
    }
}
