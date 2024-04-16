package cn.supcon.aggregate;

import cn.supcon.entity.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 简单聚合算子的实现样例：
 * flink中，聚合算子是和keyby成对出现的，通过keyby算子，拿到KeyedStream之后，才能调用对应的聚合算子
 * 本类中，使用到算子：min，max，maxBy，minBy，sum等，都是在KeyedStream类里边，这些算子不会垮key进行处理
 *
 * Flink1.17课程42课
 */
public class SimpleAggregateDemo {
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
        // sensorKS.print();
        // 根据第三个位置元素进行聚合求和，注意：传位置进行计算的，适合于Tuple类型，不适合于pojo类
        // SingleOutputStreamOperator<WaterSensor> sum = sensorKS.sum(2);
        // 根据属性字段名称进行聚合
        // SingleOutputStreamOperator<WaterSensor> sum = sensorKS.sum("vc");
        // sum.print();
        // SingleOutputStreamOperator<WaterSensor> min = sensorKS.min("vc");
        // min.print();
        // SingleOutputStreamOperator<WaterSensor> max = sensorKS.max("vc");
        // max.print();
        // 注意maxBy和max聚合算子的不同点，max不会取非比较算子的值，非比较字段保留第一次的值，maxBy会取非比较字段的值，min和minBy也是这个情况
        SingleOutputStreamOperator<WaterSensor> maxBy = sensorKS.maxBy("vc");
        maxBy.print();
        env.execute();
    }
}
