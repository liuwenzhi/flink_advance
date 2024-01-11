package cn.supcon.functions;

import cn.supcon.entity.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * 自定义map函数
 */
public class MapFunctionImpl implements MapFunction<WaterSensor, String> {
    @Override
    public String map(WaterSensor waterSensor) throws Exception {
        return waterSensor.getId();
    }
}
