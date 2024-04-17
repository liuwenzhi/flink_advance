package cn.supcon.functions;

import cn.supcon.entity.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * 自定义filter函数，在自定义底层Filter接口的实现类中，单独增加一个属性
 *
 * Flink1.17 44课
 */
public class FilterFunctionImpl implements FilterFunction<WaterSensor> {

    private String id;

    public FilterFunctionImpl(String id) {
        this.id = id;
    }

    @Override
    public boolean filter(WaterSensor waterSensor) throws Exception {
        return id.equals(waterSensor.getId());
    }
}
