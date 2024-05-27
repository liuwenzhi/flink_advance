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
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * 计数窗口演示
 * 计数窗口包括滚动和滑动窗口，没有会话窗口了
 *
 * <p>
 * Flink 1.17 70课
 */
public class CountWindowDemo {
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

        // 1.窗口分配器，三个泛型类型分别代表入参类型，key类型和窗口类型
        WindowedStream<WaterSensor, String, GlobalWindow> sensorWS =
                // sensorKS.countWindow(5); // 计数窗口，滚动窗口，窗口长度5条数据
                sensorKS.countWindow(5,2); // 计数窗口，滑动窗口，窗口长度为5条数据，滑动周期为2条数据，滑动窗口的步长=计算输出的间隔，每一个步长都有一次窗口触发输出，这里单独整理了输出说明，当前目录下代码说明
        // ProcessWindowFunction泛型四个类型含义：入参类型，输出类型，key的类型，窗口类型
        SingleOutputStreamOperator<String> process = sensorWS.process(new ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                // process 函数四个参数的含义：分组的key，context：上下文，elements：窗口存的数据，输出采集器
                long maxTs = context.window().maxTimestamp(); // 获取最大时间戳，每个窗口都一样，这里做了输出其实没多大意义
                String maxTime = DateFormatUtils.format(maxTs, "yyyy-MM-dd HH:mm:ss.SSS");
                long count = elements.spliterator().estimateSize();
                out.collect("key=" + s + "的窗口最大时间：" + maxTime + "，数据总量：" + count + "，数据信息：" + elements);
            }
        });
        process.print();
        env.execute();
    }
}
