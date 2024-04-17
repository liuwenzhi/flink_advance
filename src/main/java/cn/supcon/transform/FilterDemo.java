package cn.supcon.transform;

import cn.supcon.entity.WaterSensor;
import cn.supcon.functions.FilterFunctionImpl;
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
        // 通过匿名实现类的方式，实现写死的s1字符串对输入流对象id属性的判断
        /*sensorDS.filter(new FilterFunction<WaterSensor>() {
            @Override
            public boolean filter(WaterSensor value) throws Exception {
                return "s1".equals(value.getId());
            }
        }).print();*/
        // 通过传入变量的方式实现过滤 44课内容增加的代码
        sensorDS.filter(new FilterFunctionImpl("s1")).print();
        env.execute();
    }
}
