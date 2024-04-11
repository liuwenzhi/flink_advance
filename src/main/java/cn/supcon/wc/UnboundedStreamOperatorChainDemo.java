package cn.supcon.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 算子链的演示：代码中，flatmap和map两个算子，数据传输1对1的关系并行度也相同，如果这两个算子处理任务比较重，不适合被串在一块，适合分开
 * 或者是定位问题的时候，找到具体哪个算子有问题，适合把算子链拆开（实际场景中，90%的情况是不需要禁用算子链的）
 * Flink1.17 第27课
 */
public class UnboundedStreamOperatorChainDemo {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境，idea运行时能看到UI界面，测试用
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        // 全局禁用算子链
        // env.disableOperatorChaining();
        // 2.读取数据：socket
        DataStreamSource<String> socketDS = env.socketTextStream("172.16.2.23", 7777);
        // 基于lambda表达式实现切分和转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS.flatMap((String value, Collector<String> out) -> {
            // value参数是入参的参数值，Collector是采集器，下边是切分和转换的实现
            String[] words = value.split(" ");
            for (String word : words) {
                // 生成的对象用采集器向下游发送数据
                out.collect(word);
            }
        }).startNewChain()   // 开启一个新链条
                .returns(Types.STRING).map(word -> Tuple2.of(word, 1))   // 单独针对某个算子实现禁用算子链.disableChaining()
                .returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy((Tuple2<String, Integer> value) -> value.f0).sum(1);
        sum.print();
        env.execute();
    }
}
