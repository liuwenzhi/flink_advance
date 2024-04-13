package cn.supcon.source2;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 将集合作为源端读取数据，生产环境一般没有这么用的
 *
 * Flink1.17课程35课
 */
public class CollectionSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 单独构造一个集合对象
        // DataStreamSource<Integer> source = env.fromCollection(Arrays.asList(1, 2, 3));
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3);
        source.print();
        env.execute();
    }
}
