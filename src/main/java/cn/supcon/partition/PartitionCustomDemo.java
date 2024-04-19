package cn.supcon.partition;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义flink分区
 * <p>
 * Flink 1.17 46课
 */
public class PartitionCustomDemo {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(3);
        // 2.读取数据：socket
        DataStreamSource<String> socketDS = env.socketTextStream("172.16.2.23", 7777);
        DataStream<String> dataStream = socketDS.partitionCustom(new MyPartitioner(), new KeySelector<String, String>() {
            // keySelector两个参数：in，选择分组字段的类型，经过计算后的最终分组字段类型，这里模拟直接不计算
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        });
        dataStream.print();
        // 直接基于lambda表达式实现keySelector
        // socketDS.partitionCustom(new MyPartitioner(),r->r);
        env.execute();
    }
}
