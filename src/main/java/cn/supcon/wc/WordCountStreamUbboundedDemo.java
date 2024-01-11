package cn.supcon.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 从socket源端读取无界数据流，进行单词拆分处理
 */
public class WordCountStreamUbboundedDemo {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.读取数据：socket
        DataStreamSource<String> socketDS = env.socketTextStream("172.16.2.23", 7777);
        // 3.数据切换，转换，分组，聚合，returns算子处理泛型擦除问题，对应sum对象的二元组泛型信息
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(value -> value.f0).sum(1);
        // 4.输出
        sum.print();
        // 5.执行
        env.execute();
    }
}
