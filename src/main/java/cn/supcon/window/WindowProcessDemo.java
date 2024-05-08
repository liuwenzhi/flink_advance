package cn.supcon.window;

import cn.supcon.entity.WaterSensor;
import cn.supcon.functions.WaterSensorMapFunction;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 全窗口函数：窗口触发时才会调用一次，统一计算窗口中的所有数据
 *
 * <p>
 * Flink 1.17 67课
 */
public class WindowProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 创建kafka的源，直接在builder前边加上泛型，指定读入数据类型
        KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers("node01:9092,node02:9092,node03:9092")
                .setGroupId("group1")       // 设置flink消费者组id，这个可以随意指定一个标识名称
                .setTopics("test_20240414")
                .setValueOnlyDeserializer(new SimpleStringSchema())  // 设置反序列化，kafka是源端，flink是消费端，数据通过序列化从kafka走到flink，flink端需WaterSensorMapFunction要反序列化，这里只对value做反序列化
                .setStartingOffsets(OffsetsInitializer.latest()).build();
        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        // 读取kafka源中的数据封装成WaterSeneor类型
        SingleOutputStreamOperator<WaterSensor> sensorDS = kafkaSource.map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        // 1.窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.minutes(1)));
        SingleOutputStreamOperator<String> process = sensorWS.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
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
        process.print();
        env.execute();
    }
}
