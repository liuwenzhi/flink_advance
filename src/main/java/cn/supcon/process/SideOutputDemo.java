package cn.supcon.process;

import cn.supcon.entity.WaterSensor;
import cn.supcon.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * process算子的侧输出流
 *
 * <p>
 * Flink 1.17 98课
 */
public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("192.168.190.128", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, ts) -> element.getTs() * 1000L)
                );

        OutputTag<String> warnTag = new OutputTag<>("warn", Types.STRING);
        SingleOutputStreamOperator<WaterSensor> process = sensorDS.keyBy(sensor -> sensor.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                                // 使用侧输出流告警
                                if (value.getVc() > 10) {
                                    ctx.output(warnTag, "当前水位=" + value.getVc() + ",大于阈值10！！！");
                                }
                                // 主流正常 发送数据
                                out.collect(value);
                            }
                        }
                );
        process.print("主流");
        process.getSideOutput(warnTag).printToErr("warn");
        env.execute();
    }
}
