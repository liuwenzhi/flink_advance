package cn.supcon.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 新版本处理无界数据流word count代码
 * 本节使用了lambda表达式实现，这个时候，可能存在泛型擦除的问题（用匿名内部类或者自定义接口实现类覆写方法明确指定了入参类型和返回值类型）
 * 在采集器后边增加的算子：.returns(Types.TUPLE(Types.STRING, Types.INT)) 就是设置返回类型
 * Flink1.17 第九课
 *
 * 服务器上执行命令：nc -lk 7777，
 * 如果服务器本身没有netcat，执行命令安装下：yum install -y netcat
 */
public class WordCountStreamUnboundedDemoNew {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.读取数据：socket
        DataStreamSource<String> socketDS = env.socketTextStream("172.16.2.22", 7777);
        // 基于lambda表达式实现切分和转换
        socketDS.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                    // value参数是入参的参数值，Collector是采集器，下边是切分和转换的实现
                    String[] words = value.split(" ");
                    for (String word : words) {
                        Tuple2<String, Integer> wordAndOne = Tuple2.of(word, 1);
                        // 生成的对象用采集器向下游发送数据
                        out.collect(wordAndOne);
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy((Tuple2<String, Integer> value) -> value.f0).sum(1).print();  // 0位置元素分组，1位置元素统计
                /*.keyBy((Tuple2<String, Integer> value) -> {
                    return value.f0;
                }).sum(1).print();*/
        env.execute();
    }
}
