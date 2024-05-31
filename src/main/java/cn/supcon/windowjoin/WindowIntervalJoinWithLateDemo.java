package cn.supcon.windowjoin;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 基于时间的双流联结，带有迟到数据的Interval join，对WindowIntervalJoinDemo的进一步优化。WindowIntervalJoinDemo中设置的流是有界流
 * 如果现在做join的两个流是无界流，在匹配的时候，出现了迟到数据，比如左流某个数据关联的时间在0,2，然后事件时间是1这个记录在3这个时间点才来，这个时候被测流打印出来
 * 测试数据：
 * 7777端口输入：
 * a,4
 * a,10
 * a,3      -- 迟到数据
 * 8888端口输入：
 * a,3,3
 * a,11,11  -- 在左侧输入10之后输入这条记录，水位线提升到了7
 * a,5,5    -- 迟到数据
 * 注意一个细节：两个数据流join的时候，水位线取的是两个水位线中水位小的那个，和多任务那个是一样的
 * 如果当前数据时间 < watermark，就是迟到数据。主流不会处理。
 *
 * <p>
 * Flink 1.17 89课
 */
public class WindowIntervalJoinWithLateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env.socketTextStream("192.168.190.128", 7777).map(new MapFunction<String, Tuple2<String, Integer>>() {
            // 注意MapFunction的两个参数，一个是输入类型，一个是输出类型
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] datas = value.split(",");
                return Tuple2.of(datas[0], Integer.valueOf(datas[1]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner((value, ts) -> value.f1 * 1000L));

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env.socketTextStream("192.168.190.128", 8888).map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> map(String value) throws Exception {
                String[] datas = value.split(",");
                return Tuple3.of(datas[0], Integer.valueOf(datas[1]), Integer.valueOf(datas[2]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner((value, ts) -> value.f1 * 1000L));

        // Interval join，只支持事件时间
        // 分别做key by，key就是关联条件，泛型中两个类型是整体数据类型和key的类型
        KeyedStream<Tuple2<String, Integer>, String> ks1 = ds1.keyBy(r1 -> r1.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> ks2 = ds2.keyBy(r2 -> r2.f0);
        // 定义迟到侧输出流输出标签
        OutputTag<Tuple2<String, Integer>> ks1LateTag = new OutputTag<>("ks1-late", Types.TUPLE(Types.STRING, Types.INT));
        OutputTag<Tuple3<String, Integer, Integer>> ks2LateTag = new OutputTag<>("ks2-late", Types.TUPLE(Types.STRING, Types.INT, Types.INT));

        // 左流关联右流，between指定左右边界，负号代表时间往前，正号代表时间往后
        SingleOutputStreamOperator<String> process = ks1.intervalJoin(ks2).between(Time.seconds(-2), Time.seconds(2))
                .sideOutputLeftLateData(ks1LateTag)
                .sideOutputRightLateData(ks2LateTag)
                .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    // process算子三个参数类型，左流、右流以及最终返回类型
                    @Override
                    public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, Context ctx, Collector<String> out) throws Exception {
                        // 左流和右流的数据能匹配上的情况下，调用该方法，四个参数的含义是：左流数据，右流数据，上下文，采集器
                        out.collect(left + "<----------------->" + right);
                    }
                });
        // 通过侧输出流从ks1和ks2两个流中打印迟到数据，这些数据不会被匹配上
        process.getSideOutput(ks1LateTag).printToErr("ks1迟到数据");
        process.getSideOutput(ks2LateTag).printToErr("ks2迟到数据");
        process.print("主流");
        env.execute();
    }
}
