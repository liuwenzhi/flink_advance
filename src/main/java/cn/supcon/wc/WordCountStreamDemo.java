package cn.supcon.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 从input目录word文件读取数据，以流处理方式统计单词数量
 * Flink1.17 第七课
 */
public class WordCountStreamDemo {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.读取数据，从文件读
        DataStreamSource<String> lineDS = env.readTextFile("input/word.txt");

        // 3.处理数据:切分、转换、分组、聚合，FlatMapFunction的泛型第一个参数代表输入类型，第二个是输出类型
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                // value参数是入参的参数值，Collector是采集器，下边是切分和转换的实现
                String[] words = value.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> wordAndOne = Tuple2.of(word, 1);
                    // 生成的对象用采集器向下游发送数据
                    out.collect(wordAndOne);
                }
            }
        });

        // 分组的实现方式：提取key，匿名函数的入参是二元组，输出是key类型，字符串
        KeyedStream<Tuple2<String, Integer>, String> wordAndOneKS = wordAndOneDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        // 聚合的实现方式：通过sum方法，
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = wordAndOneKS.sum(1);

        // 4.输出数据
        sumDS.print();

        // 5.执行
        env.execute();
    }
}
