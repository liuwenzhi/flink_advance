package cn.supcon.transform;

import cn.supcon.entity.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * filter算子演示
 * <p>
 * Flink1.17课程40课
 */
public class FilterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDS = env.fromElements(new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2), new WaterSensor("s3", 3L, 3));
        sensorDS.filter(new FilterFunction<WaterSensor>() {
            @Override
            public boolean filter(WaterSensor value) throws Exception {
                return "s1".equals(value.getId());
            }
        }).print();
        env.execute();
    }
}
