package cn.supcon.watermark;

import cn.supcon.entity.WaterSensor;
import cn.supcon.functions.WaterSensorMapFunction;
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
 * 水位线功能：允许迟到的数据，数据时间小于水位线，就是乱序程度足够大
 *
 * 问题演示，输入数据如下：
 * s1,1,1
 * s1,3,3
 * s1,9,9
 * s1,13,13   水位线策略为乱序策略，设置等待时间为3秒，窗口为滚动窗口，窗口大小是10s，正向情况到接收到这条数据，应该调用窗口函数，关闭窗口了，但是之后又来了一个6，6实际就不会有窗口处理了。
 * s1,6,6
 *
 * <p>
 * Flink 1.17 84课
 */
public class WatermarkAllowLatenessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("192.168.190.128", 7777).map(new WaterSensorMapFunction());
        // 指定watermark策略，乱序的watermark策略，存在乱序数据，这里单独设置等待时间为3秒
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(
                (element, recordTimestamp) -> {
                    System.out.println("数据=" + element + "，recordTs=" + recordTimestamp);
                    return element.getTs() * 1000L;
                });
        // 注意，配合水位线功能，定义窗口的时候，需要使用事件时间窗口，之前的Processing窗口是处理时间窗口
        SingleOutputStreamOperator<WaterSensor> waterSensorMarkDS = sensorDS.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        waterSensorMarkDS.keyBy(sensor -> sensor.getId())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2)) // 针对类注释中提到的问题，提出的解决方案，允许延迟2s关窗，就是达到了水位线， 然后调用窗口函数做了输出了，再等2s再关窗，
                                                  // 2s也是事件时间，本例中，时间时间到了13s，也不会关窗，还能接收0到10s的数据，事件时间到了15s的时候，真正官场，后边再来0到10s的数据，就不等了
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            // ProcessWindowFunction抽象类四个参数的含义：入参类型，输出类型，key类型，窗口类型
            @Override
            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                // process 函数四个参数的含义：分组的key，context：上下文，elements：窗口存的数据，输出采集器
                long start = context.window().getStart();
                long end = context.window().getEnd();
                String windowStart = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
                String windowEnd = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");
                // 输出一个数据总量检查下
                long count = elements.spliterator().estimateSize();
                // 用采集器进行输出
                out.collect("key=" + s + "的窗口开始时间：" + windowStart + "，窗口结束时间：" + windowEnd + "，数据总量：" + count + "，数据信息：" + elements);
            }
        }).print();
        env.execute();
    }
}
