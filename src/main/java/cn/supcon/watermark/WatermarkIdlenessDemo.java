package cn.supcon.watermark;

import cn.supcon.entity.WaterSensor;
import cn.supcon.functions.WaterSensorMapFunction;
import cn.supcon.partition.MyPartitioner;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 多并行度下，设置水位线空闲等待时间模拟的输入数据
 * 1
 * 3
 * 5
 * 7
 * 9
 * 11
 * 13
 * 多并行度下水位线传递会导致的问题：多个上游算子中，有一个算子一直没有收到数据，或者收到很少的数据，它广播的水位线一直是一个很小的值，
 * 这种时候会导致下游算子一直取最小值，然后窗口关闭不了。水位线空闲等待时间就是为了处理这个问题。
 * <p>
 * Flink 1.17 83课
 */
public class WatermarkIdlenessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        SingleOutputStreamOperator<Integer> socketDS = env.socketTextStream("192.168.190.128", 8888)
                .partitionCustom(new MyPartitioner(), r -> r)// 设置自定义分区器，将元素本身作为分区参数
                .map(r -> Integer.parseInt(r))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Integer>forMonotonousTimestamps()
                        .withTimestampAssigner((r, ts) -> r * 1000L)
                        .withIdleness(Duration.ofSeconds(5)) // 空闲等待5s
                );
        socketDS.keyBy(r -> r % 2).window(TumblingEventTimeWindows.of(Time.seconds(10))).process(new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() {
            // ProcessWindowFunction抽象类四个参数的含义：入参类型，输出类型，key类型，窗口类型
            @Override
            public void process(Integer key, Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
                // process 函数四个参数的含义：分组的key，context：上下文，elements：窗口存的数据，输出采集器
                long start = context.window().getStart();
                long end = context.window().getEnd();
                String windowStart = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
                String windowEnd = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");
                // 输出一个数据总量检查下
                long count = elements.spliterator().estimateSize();
                // 用采集器进行输出
                out.collect("key=" + key + "的窗口开始时间：" + windowStart + "，窗口结束时间：" + windowEnd + "，数据总量：" + count + "，数据信息：" + elements);
            }
        }).print();
        env.execute();
    }
}
