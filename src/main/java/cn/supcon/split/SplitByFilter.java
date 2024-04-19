package cn.supcon.split;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 将一个整数流按照奇数偶数分流
 * 用已有的filter算子实现（比较low）
 * <p>
 * Flink 1.17 47课
 */
public class SplitByFilter {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);
        // 2.读取数据：socket
        DataStreamSource<String> socketDS = env.socketTextStream("172.16.2.23", 7777);
        // 用filter算子实现分流相对比较low，需要调用多次，每一次调用过滤出需要的结果
        socketDS.filter(value -> Integer.parseInt(value) % 2 == 0).print("偶数流");
        socketDS.filter(value -> Integer.parseInt(value) % 2 == 1).print("奇数流");
        env.execute();
    }
}
