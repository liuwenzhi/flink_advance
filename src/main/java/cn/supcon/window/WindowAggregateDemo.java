package cn.supcon.window;

import cn.supcon.entity.WaterSensor;
import cn.supcon.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Aggregate 窗口增量聚合函数
 * <p>
 * reduce增量聚合函数的问题：数据输入和输出必须是同一种类型，实际上在输入和输出之间，还会做数据的存储，reduce框架设计中间存储的类型和输入输出是同一种类型
 * 而实际上，数据流的输入类型，中间存储的类型和输出类型，可能都不相同。Aggregate增量聚合函数就是能解决这个问题。
 *
 * <p>
 * Flink 1.17 66课
 */
public class WindowAggregateDemo {
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

        // 2.窗口函数：增量聚合Aggregate，输入是WaterSensor类型，累加是整形，最终输出是字符串
        SingleOutputStreamOperator<String> aggregate = sensorWS.aggregate(new AggregateFunction<WaterSensor, Integer, String>() {

            /**
             * 创建累加器，给一个初始化值
             */
            @Override
            public Integer createAccumulator() {
                System.out.println("初始化累加器");
                return 0;
            }

            /**
             * 聚合逻辑，计算和累加，来一条数据调用一次add方法（这里与reduce不同，第一条数据过来也会执行累加方法，因为这里的累加器已经定义了初始值）
             * value是输入对象，accumulator被设置为累加器
             * 此时 accumulator相当于是水位了，把最新的数据增加到水位上
             */
            @Override
            public Integer add(WaterSensor value, Integer accumulator) {
                System.out.println("调用累加方法，value=" + value);
                return accumulator + value.getVc();
            }

            /**
             * 窗口到期，返回结果
             */
            @Override
            public String getResult(Integer accumulator) {
                System.out.println("调用输出方法");
                return accumulator.toString();
            }

            /**
             * 会话窗口使用merge函数，本例为滚动窗口暂时用不到
             */
            @Override
            public Integer merge(Integer a, Integer b) {
                return null;
            }
        });
        aggregate.print();
        env.execute();
    }
}
