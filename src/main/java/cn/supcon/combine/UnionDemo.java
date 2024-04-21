package cn.supcon.combine;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Union合流方案：一次可以合并多条流，流的数据类型必须一致
 * <p>
 * Flink 1.17 49课
 */
public class UnionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3);
        DataStreamSource<Integer> source2 = env.fromElements(11, 22, 33);
        DataStreamSource<String> source3 = env.fromElements("111", "222", "333");
        // 只有同种类型的流才能合并
        // DataStream<Integer> union1 = source1.union(source2).union(source3.map(value -> Integer.parseInt(value)));
        DataStream<Integer> union1 = source1.union(source2, source3.map(value -> Integer.parseInt(value)));
        union1.print();
        env.execute();
    }
}
