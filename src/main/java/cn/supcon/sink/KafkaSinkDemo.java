package cn.supcon.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * 写数据到kafka目的端
 * 代码使用精准一次方式写入到kafka，如果不设置精准一次写入，可以把注释说明精准一次部分的代码去掉
 */
public class KafkaSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置写入kafka为精准一次写入，必须要开启checkpoint
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        SingleOutputStreamOperator<String> sensorDS = env.socketTextStream("172.16.2.23", 7777);
        // 自定义一个kafka sink
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder().setBootstrapServers("172.16.2.62:9092").setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("topic_test")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE) // 设置kafka一致性级别，如果是精准一次，必须要设置事务前缀
                .setTransactionalIdPrefix("supcon-")
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10 * 60 * 1000 + "") // 精准一次写入必须要设置事务超时时间，大于checkpoint时间间隔，不超过15分钟的一个时间信息，单位是毫秒
                .build();
        sensorDS.sinkTo(kafkaSink);
        env.execute();
    }
}
