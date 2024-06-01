package cn.supcon.process;

import cn.supcon.entity.WaterSensor;
import cn.supcon.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 键控流定时器与时间演示代码
 * 本类中演示的key和watermark没有关系，如果是采用任务时间的定时器，是通过水位线触发的
 * 注意一个细节：watermark >= 注册时间
 * watermark = 当前最大事件时间 - 等待时间（乱序数据涉及到） - 1ms，因为涉及到 -1ms，所以会推迟一条数据
 * 比如：5s的定时器，如果等待时间=3s，watermark = 8s - 3s - 1ms = 4999ms 不会触发5s的定时器
 * 需要watermark = 9s - 3s -1ms = 5999ms
 * 这里和之前调用watermark演示效果可能有不同，因为process算子一次只能处理一条数据，而watermark也是一条数据。
 * 当process算子处理当前这条数据的时候，并不能拿到当前这条数据的watermark数据（当前数据之后一条数据），只能拿到上一次计算的watermark
 * 具体逻辑参考93课内容说明
 *
 * <p>
 * Flink 1.17 91课
 */
public class KeyedProcessTimerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("192.168.190.128", 7777).map(new WaterSensorMapFunction());
        // 指定watermark策略，乱序的watermark策略，存在乱序数据，这里单独设置等待时间为3秒，本例中10秒的窗口，等到13秒的数据来了之后，再调用窗口函数，进行关窗
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(
                (element, recordTimestamp) -> {
                    System.out.println("数据=" + element + "，recordTs=" + recordTimestamp);
                    return element.getTs() * 1000L;
                });
        // 注意，配合水位线功能，定义窗口的时候，需要使用事件时间窗口，之前的Processing窗口是处理时间窗口
        SingleOutputStreamOperator<WaterSensor> waterSensorMarkDS = sensorDS.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);
        KeyedStream<WaterSensor, String> sensorKS = waterSensorMarkDS.keyBy(sensor -> sensor.getId());
        SingleOutputStreamOperator<String> process = sensorKS.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            // KeyedProcessFunction三个泛型：key类型，输入类型，输出类型

            // 步骤1：定义一个定时器
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                // 数据中提取出来的事件时间
                Long timestamp = ctx.timestamp();
                //  重点：提取定时器，类似闹钟，闹钟响了，触发onTimer方法
                TimerService timerService = ctx.timerService();
                // 注册定时器：事件时间
                timerService.registerEventTimeTimer(5000L);
                String currentKey = ctx.getCurrentKey();
                System.out.println(currentKey + "，当前时间是：" + timestamp + "，注册了一个5s的定时器");
                // 获取当前process的watermark，不是整条流的watermark
                long currentWatermark = timerService.currentWatermark();
                System.out.println("当前数据=" + value + "，当前watermark=" + currentWatermark);
            }

            // 步骤2：定义一个定时器触发的方法
            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                // 上边方法来一条数据，注册一个定时器，注册了多个5s的定时器，flink会自动去重，最终执行的5s定时器只有一个
                super.onTimer(timestamp, ctx, out);
                String currentKey = ctx.getCurrentKey();
                System.out.println(currentKey + "，现在时间是：" + timestamp + "，定时器出发了");
            }
        });

        // 最后是否做打印，无所谓
        process.print();
        env.execute();
    }
}
