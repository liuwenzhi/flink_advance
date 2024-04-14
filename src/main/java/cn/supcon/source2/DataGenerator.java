package cn.supcon.source2;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 数据生成器
 * <p>
 * Flink1.17课程36课
 */
public class DataGenerator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 如果有n个并行度，最大值设为a，则生成器的生成规则时：将数值均分成n份，每一份a/n，比如并行度2，最大值100，则第一个并行度0~49，第二个并行度50~99
        env.setParallelism(2);
        // 生成器源调用这个方法里边四个参数，第一个是生成的内容(入参Long类型不能改)，第二个是自动生成数字序列（从0自增）的最大值-1，第三个是限速策略，第四个是返回值类型
        DataGeneratorSource dataGeneratorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            // 这里的value是从1开始自增的一个数字序列
            @Override
            public String map(Long value) throws Exception {
                return "Number:" + value;
            }
        }, 10, // 传参：Long.MAX_VALUE 可以模拟出一个无界流，程序不停止的效果，传10，输出十条记录就完事了
                RateLimiterStrategy.perSecond(1),  // 每秒钟1条记录
                Types.STRING);
        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator").print();
        env.execute();
    }
}
