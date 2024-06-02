package cn.supcon.state;

import cn.supcon.entity.WaterSensor;
import cn.supcon.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 按键分区状态：值状态
 * 需求：检测每种传感器的水位值，如果连续的两个水位值超过10，就输出报警。每种传感器可以直接通过WaterSensor的id属性来区分
 *
 * <p>
 * Flink 1.17 100课
 */
public class KeyedValueStateDemo {
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
        sensorDS.keyBy(r -> r.getId()).process(new KeyedProcessFunction<String, WaterSensor, Object>() {
            // 存一个状态信息，不能直接在这里初始化，如果状态定义成一个普通变量，则会导致多个key的数据混合进行计算，乱套了
            ValueState<Integer> lastVcState;

            // 状态的初始化，需要在open生命周期方法中去定义
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                lastVcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVcState", Types.INT()));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<Object> out) throws Exception {
                // 1.取出上一条数据的水位值（状态）
                int lastVc = lastVcState.value() == null ? 0 : lastVcState.value();
                // 2.业务处理
                if (Math.abs(value.getVc() - lastVc) > 10) {
                    out.collect("传感器id：" + value.getId() + ",当前水位值=" + value.getVc() + ",与上一条水位值=" + lastVc + ",相差超过10！！！！");
                }
                // 3.保存更新状态里的水位值
                lastVcState.update(value.getVc());
            }
        }).print();
        env.execute();

    }

}
