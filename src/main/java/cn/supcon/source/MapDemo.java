package cn.supcon.source;

import cn.supcon.entity.WaterSensor;
import cn.supcon.functions.MapFunctionImpl;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义Map函数代码样例
 */
public class MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );
        // 方式一：匿名实现类
        /*SingleOutputStreamOperator<String> map = sensorDS.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor waterSensor) throws Exception {
                return waterSensor.getId();
            }
        });*/
        // 方式二：lambda表达式
        // SingleOutputStreamOperator<String> map = sensorDS.map(sensor -> sensor.getId());
        // 方式三：定义一个类，实现map function
        // SingleOutputStreamOperator<String> map = sensorDS.map(new MyMapFunction());
        // 正规写法：单独定义一个外部类
        SingleOutputStreamOperator<String> map = sensorDS.map(new MapFunctionImpl());
        map.print();
        env.execute();
    }

    /**
     * 注意main方法是静态方法，MyMapFunction这个类在main方法中被调用也要写成静态的
     */
    public static class MyMapFunction implements MapFunction<WaterSensor, String> {
        @Override
        public String map(WaterSensor waterSensor) throws Exception {
            return waterSensor.getId();
        }
    }
}
