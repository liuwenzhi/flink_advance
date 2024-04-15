package cn.supcon.transform;

import cn.supcon.entity.WaterSensor;
import cn.supcon.functions.MapFunctionImpl;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * map算子演示，map算子的功能：一进一出，一对一输出，对输入数据进行改造再输出
 * <p>
 * Flink1.17课程39课
 */
public class MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDS = env.fromElements(new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2), new WaterSensor("s3", 3L, 3));
        // map参数是一个接口，可以通过匿名实现类（函数式编程），lambda表达式，或者自定义内部类实现具体接口方法来实现
        SingleOutputStreamOperator<String> map = sensorDS.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor value) throws Exception {
                return value.getId();
            }
        });
        map.print();
        // lambda表达式实现,lambda表达式中的参数可以随便取
        SingleOutputStreamOperator<String> map1 = sensorDS.map(sensor -> sensor.getId());
        map1.print();
        // 自定义实现类方式，方便公用（当前类文件中创建一个静态类，涉及到类上下文引用，必须要用静态类）
        SingleOutputStreamOperator<String> map2 = sensorDS.map(new MyMapFunction());
        map2.print();
        // 最规范的写法，定义一个公共的类
        SingleOutputStreamOperator<String> map3 = sensorDS.map(new MapFunctionImpl());
        map3.print();
        env.execute();
    }

    static class MyMapFunction implements MapFunction<WaterSensor, String> {
        @Override
        public String map(WaterSensor value) throws Exception {
            return value.getId();
        }
    }
}
