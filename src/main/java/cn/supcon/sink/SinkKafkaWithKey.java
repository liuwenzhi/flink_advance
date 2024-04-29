package cn.supcon.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * KafkaSinkDemo的补充
 * 自定义序列化器，包括对kafka的key和value值的序列化
 * 之前传输的测试数据类似这种：
 * 1,1,a
 * 2,2,b
 * 3,3,c
 * 序列化也是针对于数据本身，现在是给这个数据加一个key，比如1,1,a的key是1,然后给这个key再做一次序列化
 * 具体思路：自定义一个序列化器：
 * 1、实现一个接口，重写序列化方法
 * 2、指定key，转成字节数组
 * 3、指定value，转成字节数组
 * 4、返回一个ProducerRecord对象，把key、value放进去
 *
 * Flink 1.17 56课
 */
public class SinkKafkaWithKey {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置写入kafka为精准一次写入，必须要开启checkpoint
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        SingleOutputStreamOperator<String> sensorDS = env.socketTextStream("172.16.2.23", 7777);
        // 自定义一个kafka sink
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder().setBootstrapServers("172.16.2.62:9092").setRecordSerializer(
                        // 通过匿名实现类的方式实现该接口
                        new KafkaRecordSerializationSchema<String>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(String element, KafkaSinkContext kafkaSinkContext, Long aLong) {
                                String[] datas = element.split(",");
                                byte[] key = datas[0].getBytes(StandardCharsets.UTF_8);
                                byte[] value = element.getBytes(StandardCharsets.UTF_8);
                                return new ProducerRecord<>("topic_test", key, value);
                            }
                        })
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE) // 设置发送保证，kafka一致性级别（精准一次，至少一次等），如果是精准一次，必须要设置事务前缀
                .setTransactionalIdPrefix("supcon-")
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10 * 60 * 1000 + "") // 精准一次写入必须要设置事务超时时间，大于checkpoint时间间隔，不超过15分钟的一个时间信息，单位是毫秒
                .build();
        sensorDS.sinkTo(kafkaSink);
        env.execute();
    }
}
