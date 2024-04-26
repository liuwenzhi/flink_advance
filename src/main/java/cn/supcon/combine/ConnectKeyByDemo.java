package cn.supcon.combine;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 需求：连接两条流，输出能根据id匹配上的数据，效果类似sql里边的inner join效果
 * <p>
 * Flink 1.17 51课
 */
public class ConnectKeyByDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 针对于该需求，如果设置并行度为1，合并流的时候，都在一个分区里边，如果设置为2以上，则必须使用keyby先分好组，再进行下一步算子操作
        env.setParallelism(2);

        DataStreamSource<Tuple2<Integer, String>> source1 = env.fromElements(
                Tuple2.of(1, "a1"),
                Tuple2.of(1, "a2"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c")
        );
        DataStreamSource<Tuple3<Integer, String, Integer>> source2 = env.fromElements(
                Tuple3.of(1, "aa1", 1),
                Tuple3.of(1, "aa2", 2),
                Tuple3.of(2, "bb", 1),
                Tuple3.of(3, "cc", 1)
        );
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connect = source1.connect(source2);

        // 多并行度下，需要根据关联条件进行keyby，保证相同key的数据到同一个并行度线程中去，如果是单并行度，直接哟个connect算子做接下来的计算即可
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connectKyBy = connect.keyBy(s1 -> s1.f0, s2 -> s2.f0); // 两个参数，要么用lambda，要么自定义接口实现类
        /**
         * 实现相互匹配的效果：
         * 1、两条流，不一定谁的数据先来，
         * 2、每条流，有数据来，存到一个变量中
         * 3、每条流有数据来的时候，除了存在变量中，不知道对方是否有匹配的数据，要去另外一条流存的变量中查找是否有匹配上的
         * 实际这是状态计算，后边课程会涉及到。
         * 基于以上的思路，两条流，不一定谁先到，算子中覆写的两个方法都要进行判断处理，a先到了，没有匹配，b到了就和a匹配上了。或者b到了没有匹配，a到了就找到了b
         */
        SingleOutputStreamOperator<String> process = connectKyBy.process(new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>() {
            //定义source1和source2两个流的缓存
            Map<Integer, List<Tuple2<Integer, String>>> s1Cache = new HashMap<>();
            Map<Integer, List<Tuple3<Integer, String, Integer>>> s2Cache = new HashMap<>();
            @Override
            public void processElement1(Tuple2<Integer, String> value, Context ctx, Collector<String> out) throws Exception {
                // 第一步：source1的数据来，存到变量中
                Integer id = value.f0;
                if (!s1Cache.containsKey(id)) {
                    // 1.1 第一条数据，初始化 value的list，放入 hashmap
                    List<Tuple2<Integer, String>> s1Values = new ArrayList<>();
                    s1Values.add(value);
                    s1Cache.put(id, s1Values);
                } else {
                    // 1.2 不是第一条，直接添加到 list中
                    s1Cache.get(id).add(value);
                }
                // 第二步：去s2Cache中根据id查找，是否能匹配上的，匹配上就输出，没有就不输出
                if (s2Cache.containsKey(id)) {
                    for (Tuple3<Integer, String, Integer> s2Element : s2Cache.get(id)) {
                        out.collect("s1:" + value + "<--------->s2:" + s2Element);
                    }
                }
            }

            @Override
            public void processElement2(Tuple3<Integer, String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                Integer id = value.f0;
                // TODO 1.来过的s2数据，都存起来
                if (!s2Cache.containsKey(id)) {
                    // 1.1 第一条数据，初始化 value的list，放入 hashmap
                    List<Tuple3<Integer, String, Integer>> s2Values = new ArrayList<>();
                    s2Values.add(value);
                    s2Cache.put(id, s2Values);
                } else {
                    // 1.2 不是第一条，直接添加到 list中
                    s2Cache.get(id).add(value);
                }
                //TODO 2.根据id，查找s1的数据，只输出匹配上的数据
                if (s1Cache.containsKey(id)) {
                    for (Tuple2<Integer, String> s1Element : s1Cache.get(id)) {
                        out.collect("s1:" + s1Element + "<--------->s2:" + value);
                    }
                }
            }
        });
        process.print();
        env.execute();
    }
}
