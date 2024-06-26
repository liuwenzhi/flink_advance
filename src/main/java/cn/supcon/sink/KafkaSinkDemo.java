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
 * 注意：如果要使用精准一次 写入kafka，需要满足一下条件，缺一不可：
 * 1、开启checkpoint（后续课程介绍）
 * 2、设置事务前缀
 * 3、设置事务超时时间：  checkpoint间隔  < 事务超时时间  <  max的15分钟
 *
 * Flink 1.17 55课
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
                                .setValueSerializationSchema(new SimpleStringSchema())   // 设置value序列化器
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE) // 设置发送保证，kafka一致性级别（精准一次，至少一次等），如果是精准一次，必须要设置事务前缀
                .setTransactionalIdPrefix("supcon-")
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10 * 60 * 1000 + "") // 精准一次写入必须要设置事务超时时间，大于checkpoint时间间隔，不超过15分钟的一个时间信息，单位是毫秒
                .build();
        sensorDS.sinkTo(kafkaSink);
        env.execute();
    }
}
