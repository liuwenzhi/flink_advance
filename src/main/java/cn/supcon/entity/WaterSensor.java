package cn.supcon.entity;

import lombok.*;
import lombok.experimental.Accessors;

import java.util.Objects;

/**
 * 模拟数据流水实体类：水位传感器
 */

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode
@Builder
@Accessors(chain = true)
public class WaterSensor {

    /**
     * 水位传感器类型
     */
    private String id;

    /**
     * 传感器记录时间戳
     */
    private Long ts;

    /**
     * 水位记录
     */
    private Integer vc;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WaterSensor that = (WaterSensor) o;
        return Objects.equals(id, that.id) && Objects.equals(ts, that.ts) && Objects.equals(vc, that.vc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, ts, vc);
    }
}
