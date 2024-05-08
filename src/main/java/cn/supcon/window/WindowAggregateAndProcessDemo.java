package cn.supcon.window;

import cn.supcon.entity.WaterSensor;
import cn.supcon.functions.WaterSensorMapFunction;
import org.apache.commons.lang.time.DateFormatUtils;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Aggregate窗口聚合函数和Process全窗口函数组合使用
 *
 * <p>
 * Flink 1.17 68课
 */
public class WindowAggregateAndProcessDemo {
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

        /*
        * 增量聚合Agg+全窗口process
        * 1、增量聚合处理函数：来一条，算一条
        * 2、窗口触发时，增量聚合的结果（只有一条）传递给 全窗口函数
        * 3、经过全窗口函数的处理包装后，输出
        *
        * 结合两者的优点：
        * 1、增量聚合：来一条，算一条，存储中间计算的结果，占用空间烧
        * 2、全窗口函数：可以通过上下文，实现灵活的功能
        */
        SingleOutputStreamOperator<String> aggregate = sensorWS.aggregate(new MyAgg(), new MyProcess());
        aggregate.print();
        env.execute();
    }

    /**
     * 自定义增量聚合逻辑
     */
    private static class MyAgg implements AggregateFunction<WaterSensor, Integer, String> {

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
    }

    /**
     * 自定义窗口函数，该泛型类的入参是增量聚合函数的结果类型
     *
     * ProcessWindowFunction泛型含义：入参类型，输出类型，key类型，窗口类型
     */
    private static class MyProcess extends ProcessWindowFunction<String, String, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
            // process 函数四个参数的含义：分组的key，context：上下文，elements（本例中只有一条记录，agg聚合完的结果）：窗口存的数据，输出采集器
            long start = context.window().getStart();
            long end = context.window().getEnd();
            String windowStart = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
            String windowEnd = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");
            // 输出一个数据总量检查下
            long count = elements.spliterator().estimateSize();
            // 用采集器进行输出
            out.collect("key=" + s + "的窗口开始时间：" + windowStart + "，窗口结束时间：" + windowEnd + "，数据总量：" + count + "，数据信息：" + elements);
        }
    }
}
