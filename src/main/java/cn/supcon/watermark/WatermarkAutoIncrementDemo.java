package cn.supcon.watermark;

import cn.supcon.entity.WaterSensor;
import cn.supcon.functions.WaterSensorMapFunction;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 水位线代码演示：有序数据搭配窗口
 * 基于kafka消息队列做水位线，没有看到效果，参考MonoDemo通过socket方式实现水位线
 *
 * <p>
 * Flink 1.17 77课
 */
public class WatermarkAutoIncrementDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers("node01:9092,node02:9092,node03:9092")
                .setGroupId("group1")
                .setTopics("test_20240414")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest()).build();
        // 指定Watermark策略，这里针对于事件事件连续自增的情况，注意：forMonotonousTimestamps是一个泛型方法，需要在方法名称前指定用于提取时间戳的泛型对象类型
        WatermarkStrategy<String> commonWatermarkStrategy = WatermarkStrategy.<String>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            // 提取一个序列化的时间戳参数（单位是毫秒），如果没有指定，默认recordTimestamp
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                System.out.println("数据=" + element + "，recordTs=" + recordTimestamp);
                String[] datas = element.split(",");
                return Long.valueOf(datas[1]) * 1000;
            }
        });
        DataStreamSource<String> kafkaSource = env.fromSource(source, commonWatermarkStrategy, "Kafka Source");
        // 下边这种通过对方方式指定watermark，因为和kafka读取源的时候有矛盾的地方暂时用不了，读取源的时候需要指定watermark策略
        /*WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy.<WaterSensor>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            // 提取一个序列化的时间戳参数（单位是毫秒），如果没有指定，默认recordTimestamp
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                System.out.println("数据=" + element + "，recordTs=" + recordTimestamp);
                return element.getTs() * 1000;
            }
        });
        SingleOutputStreamOperator<WaterSensor> sensorDS = kafkaSource.map(new WaterSensorMapFunction()).assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);*/
        // 读取kafka源中的数据封装成WaterSeneor类型
        SingleOutputStreamOperator<WaterSensor> sensorDS = kafkaSource.map(new WaterSensorMapFunction());

        // 注意，定义窗口的时候，需要使用事件窗口
        sensorDS.keyBy(sensor -> sensor.getId()).window(TumblingEventTimeWindows.of(Time.seconds(10))).process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
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
