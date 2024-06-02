package cn.supcon.state;

import cn.supcon.entity.WaterSensor;
import cn.supcon.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 按键分区状态：聚合状态
 * 与103课规约算子主要区别是入参和最终输出结果可以不同，和之前的Agg聚合函数非常相似
 *
 * 需求：计算每种传感器的平均水位
 *
 * <p>
 * Flink 1.17 104课
 */
public class KeyedAggregatingStateDemo {
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

        sensorDS.keyBy(r -> r.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {

                            AggregatingState<Integer, Double> vcAvgAggregatingState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                vcAvgAggregatingState = getRuntimeContext()
                                        .getAggregatingState(
                                                // AggregatingStateDescriptor三个泛型，入参，累加器类型，输出结果类型
                                                new AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double>(
                                                        "vcAvgAggregatingState",
                                                        new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {

                                                            // 初始化累加器，给一个0,0二元组
                                                            @Override
                                                            public Tuple2<Integer, Integer> createAccumulator() {
                                                                return Tuple2.of(0, 0);
                                                            }

                                                            // 算子方法，值相加，个数增1
                                                            @Override
                                                            public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                                                return Tuple2.of(accumulator.f0 + value, accumulator.f1 + 1);
                                                            }

                                                            // 最后输出结果，数值/总数 再转个浮点数类型，这里通过分子*1D转double类型
                                                            @Override
                                                            public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                                                return accumulator.f0 * 1D / accumulator.f1;
                                                            }

                                                            // merge方法用不到，该方法用于会话窗口
                                                            @Override
                                                            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
//                                                                return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                                                                return null;
                                                            }
                                                        },
                                                        Types.TUPLE(Types.INT, Types.INT)) // 最后一个参数是累加器类型信息
                                        );
                            }

                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                                // 将 水位值 添加到  聚合状态中
                                vcAvgAggregatingState.add(value.getVc());
                                // 从 聚合状态中 获取结果
                                Double vcAvg = vcAvgAggregatingState.get();

                                out.collect("传感器id为" + value.getId() + ",平均水位值=" + vcAvg);

//                                vcAvgAggregatingState.get();    // 对 本组的聚合状态 获取结果
//                                vcAvgAggregatingState.add();    // 对 本组的聚合状态 添加数据，会自动进行聚合
//                                vcAvgAggregatingState.clear();  // 对 本组的聚合状态 清空数据
                            }
                        }
                )
                .print();

        env.execute();
    }
}
