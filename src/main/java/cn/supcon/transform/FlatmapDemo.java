package cn.supcon.transform;

import cn.supcon.entity.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * flatmap 扁平化算子演示，flatmap：一进多出
 * <p>
 * 功能：如果输入的数据是sensor_1，只打印vc；如果输入的数据是sensor_2，既打印ts又打印vc
 * map算子和flatmap接口方法的区别，map带有一个唯一的返回值类型，flapmap没有返回值，依靠采集器输出数据到下游
 * Flink1.17课程40课
 */
public class FlatmapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDS = env.fromElements(new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2), new WaterSensor("s3", 3L, 3));
        // s1 一进一出，s2 一进二处，s3 一进0出（类似过滤效果）
        sensorDS.flatMap(new FlatMapFunction<WaterSensor, String>() {
            @Override
            public void flatMap(WaterSensor value, Collector<String> out) throws Exception {
                switch (value.getId()) {
                    case "s1":
                        out.collect(value.getVc() + "");
                        break;
                    case "s2":
                        out.collect(value.getTs() + "");
                        out.collect(value.getVc() + "");
                        break;
                    default:
                        break;
                }
            }
        }).print();
        env.execute();
    }
}
