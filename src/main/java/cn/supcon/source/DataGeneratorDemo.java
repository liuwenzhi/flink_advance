package cn.supcon.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义数据生成器代码样例
 */
public class DataGeneratorDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long aLong) throws Exception {
                        return "Number:" + aLong;
                    }
                },
                10, // 限制总共产生10条记录，这个时候是个有界流的效果，可以使用Long的最大值模拟无界流效果
                RateLimiterStrategy.perSecond(1), // 产生数据的速率，1秒钟1个
                Types.STRING   // 产生数据的返回类型
        );
        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator").print();
        env.execute();
    }
}
