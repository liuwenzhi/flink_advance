package cn.supcon.aggregate;

import cn.supcon.entity.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * reduce聚合算子代码样例：
 * reduce算子可以实现一些自定义的计算，也必须是子keyby算子之后调用，实现两两元素聚合，元素的类型和最后聚合结果值的类型要一致，实际有一定的限制
 * reduce算子会把每一次的计算结果存下来，自己维护，属于有状态的计算
 * Flink1.17课程43课
 */
public class ReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDS = env.fromElements(new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11), new WaterSensor("s2", 2L, 2), new WaterSensor("s3", 3L, 3));
        // 按照key来进行分组
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });

        // reduce算子的输入类型和输出类型必须要一致
        sensorKS.reduce(new ReduceFunction<WaterSensor>() {
            // 自定义的聚合逻辑非常灵活，但是需要保证入参和出参类型一致，value1是上一次聚合计算的结果，存状态，value2是现在来的数据，数据流中第一条数据过来的时候，是不会调用reduce的
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                return new WaterSensor(value1.getId(), value2.getTs(), value1.getVc() + value2.getVc());
            }
        }).print();
        env.execute();
    }
}
