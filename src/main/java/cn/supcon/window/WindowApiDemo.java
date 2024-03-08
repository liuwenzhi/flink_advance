package cn.supcon.window;

import cn.supcon.entity.WaterSensor;
import cn.supcon.functions.WaterSensorMapFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WindowApiDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 没有key by
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("172.16.2.23", 7777).map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());
        // 1、开窗：指定窗口分配器：时间，计数，滚动，滑动，会话

        // 没有keyby的窗口：窗口内的所有数据，进入同一个子任务，并行度只能为1
        // sensorDS.windowAll();
        // 有keyby的窗口：每个key上都定义了一组窗口，各自独立的进行统计计算
        // sensorKS.window();


        // 2、指定窗口函数：窗口内数据的计算逻辑


        env.execute();
    }
}
