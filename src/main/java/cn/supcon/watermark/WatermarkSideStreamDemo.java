package cn.supcon.watermark;

import cn.supcon.entity.WaterSensor;
import cn.supcon.functions.WaterSensorMapFunction;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 水位线功能遇到的问题比84课还要严重（WatermarkAllowLatenessDemo），就是已经延迟时间，但是还有迟到的数据，这种情况相对比较极端
 * 可以使用窗口侧输出流的方式，对严重迟到的数据进行处理
 *
 * <p>
 * Flink 1.17 85课
 */
public class WatermarkSideStreamDemo {
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
        OutputTag<WaterSensor> lateTag = new OutputTag<>("late-data", Types.POJO(WaterSensor.class));
        SingleOutputStreamOperator<String> process = waterSensorMarkDS.keyBy(sensor -> sensor.getId())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(lateTag) // 关窗后的迟到数据，放入侧输出流
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
                });
        // 打印侧输出流，打印迟到数据
        process.getSideOutput(lateTag).printToErr();
        // 打印主流
        process.print();
        env.execute();
    }
}
