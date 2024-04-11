package cn.supcon.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 无界流统计单词个数，增加：
 * 设置算子并行度，
 * 设置WebUI运行环境，注意这里需要增加maven依赖（具体看pom文件）
 * Flink1.17 第25课
 */
public class WordCountStreamTest {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境，idea运行时能看到UI界面，测试用
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        // 设置全局并行度
        env.setParallelism(3);
        // 2.读取数据：socket
        DataStreamSource<String> socketDS = env.socketTextStream("172.16.2.23", 7777);
        // 基于lambda表达式实现切分和转换
        socketDS.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
            // value参数是入参的参数值，Collector是采集器，下边是切分和转换的实现
            String[] words = value.split(" ");
            for (String word : words) {
                Tuple2<String, Integer> wordAndOne = Tuple2.of(word, 1);
                // 生成的对象用采集器向下游发送数据
                out.collect(wordAndOne);
            }
        }).setParallelism(2) // 局部算子设置的并行度优先级高于env设置的全局优先级
                .returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy((Tuple2<String, Integer> value) -> value.f0).sum(1).print();  // 0位置元素分组，1位置元素统计
        env.execute();
    }
}
