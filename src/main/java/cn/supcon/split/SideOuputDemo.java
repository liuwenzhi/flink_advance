package cn.supcon.split;

import cn.supcon.entity.WaterSensor;
import cn.supcon.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 分流
 * 需求：将输入的s1和s2流分开，其它的s3，s4这些继续按照原来的流往下走
 * 实际应用场景：主流是正常数据，测流是会产生告警的数据单独输出一个流
 *
 * Flink 1.17 48课
 */
public class SideOuputDemo {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        // 2.读取数据：socket，单独做下处理，进来是一个字符串，出来是watersensor对象
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("172.16.2.23", 7777).map(new WaterSensorMapFunction());

        // id为s1放到侧输出流s1中，通过上下文对象实现，OutputTag对象传入的两个参数：侧输出流的名称，放入侧输出流的对象类型
        OutputTag<WaterSensor> s1Tag = new OutputTag<>("s1", Types.POJO(WaterSensor.class));

        // id为s2放到侧输出流s2中
        OutputTag<WaterSensor> s2Tag = new OutputTag<>("s2", Types.POJO(WaterSensor.class));

        // 注意：process是一个底层的算子，map，filter等都是调用的这个算子
        SingleOutputStreamOperator<WaterSensor> process = sensorDS.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            // ProcessFunction泛型两个参数，一个是入参类型，一个是主流的输出类型，不用管分流的类型信息
            @Override
            public void processElement(WaterSensor value, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                switch (value.getId()) {
                    case "s1":
                        // ctx的output方法两个参数，第一个是outputTag对象，第二个参数是放入侧输出流的数据
                        ctx.output(s1Tag, value);
                        break;
                    case "s2":
                        ctx.output(s2Tag, value);
                        break;
                    default:
                        out.collect(value); // 主流的处理：直接将整条数据放到采集器里边
                        break;
                }
            }
        });
        // 打印主流数据
        process.print("main");
        // 从主流中根据标签，打印侧输出流s1
        // process.getSideOutput(s1Tag).print("s1");
        // 红色错误打印效果
        process.getSideOutput(s1Tag).printToErr("s1");
        // 打印侧输出流s2
        process.getSideOutput(s2Tag).print("s2");
        env.execute();
    }
}
