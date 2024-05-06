package cn.supcon.window;

import cn.supcon.entity.WaterSensor;
import cn.supcon.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 窗口API概念
 *
 * <p>
 * Flink 1.17 63课
 */
public class WindowApiDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 没有key by，优化原来使用socket方式读取数据，改为通过kafka源
        // SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("172.16.2.23", 7777).map(new WaterSensorMapFunction());
        // 创建kafka的源，直接在builder前边加上泛型，指定读入数据类型
        KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers("node01:9092,node02:9092,node03:9092")
                .setGroupId("group1")       // 设置flink消费者组id，这个可以随意指定一个标识名称
                .setTopics("test_20240414")
                .setValueOnlyDeserializer(new SimpleStringSchema())  // 设置反序列化，kafka是源端，flink是消费端，数据通过序列化从kafka走到flink，flink端需WaterSensorMapFunction要反序列化，这里只对value做反序列化
                .setStartingOffsets(OffsetsInitializer.latest()).build();
        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        SingleOutputStreamOperator<WaterSensor> sensorDS = kafkaSource.map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());
        // 1、开窗：指定窗口分配器（对应于windowAll函数和window函数）：时间，计数，滚动，滑动，会话

        // 没有keyby的窗口：窗口内的所有数据，进入同一个子任务，并行度只能为1
        // sensorDS.windowAll();
        // 有keyby的窗口：每个key上都定义了一组窗口，各自独立的进行统计计算
        // sensorKS.window()
        // 基于时间的滚动窗口，时间长度是1分钟
        // sensorKS.window(TumblingProcessingTimeWindows.of(Time.minutes(1)));
        // 基于时间的滑动窗口，窗口时间是1分钟，滑动步长是30s，窗口和窗口之间有30s的重合
        // sensorKS.window(SlidingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(30)));
        // 会话窗口，只有时间类型的，超过多长时间没有来数据，那么就把前边的数据都划分到一个窗口中，下边是定义一个超时间隔为5s的会话窗口
        // sensorKS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));

        // 基于计数的滚动窗口，每来5条数据，计算一次，此时这个窗口和时间就没有关系了
        // sensorKS.countWindow(5);
        // 基于计数的滑动窗口，每来5条数据，计算一次，滑动步长=2，前一个窗口和后一个窗口之间间隔2个数据
        // sensorKS.countWindow(5, 2);
        // 全局窗口，计数窗口的底层用的是这个，需要自定义一个触发器，这个一般用的很少
        // sensorKS.window(GlobalWindows.create());

        // 2、指定窗口函数：窗口内数据的计算逻辑
        WindowedStream<WaterSensor, String, TimeWindow> window = sensorKS.window(TumblingProcessingTimeWindows.of(Time.minutes(1)));
        // 增量聚合函数：reduce，aggregate，来一条，计算一条，窗口触发的时候输出结果，类似用水桶装水，水源源不断的流进来，然后不断地监测水位信息，到达窗口触发条件的时候做输出

        // 全窗口函数：apply，process，数据来了，不计算，窗口触发的时候，计算输出结果，类似水在流进水桶的过程中，不会去测量具体的水位，只管接水，不管测量，一直到窗口触发的时候（比如5分钟到了），再去量一下


        env.execute();
    }
}
