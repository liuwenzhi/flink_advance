package cn.supcon.functions;

import cn.supcon.entity.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * 自定义一个map函数类，根据String类型入参过滤出WaterSensor对象
 *
 * Flink1.17 48课
 */
public class WaterSensorMapFunction implements MapFunction<String, WaterSensor> {
    @Override
    public WaterSensor map(String value) throws Exception {
        String[] datas = value.split(",");
        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
    }
}
