package cn.supcon.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 写入文件的sink算子
 * <p>
 * Flink 1.17 54课
 */
public class FileSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataGeneratorSource dataGeneratorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "Number:" + value;
            }
        }, 100,
                RateLimiterStrategy.perSecond(1),
                Types.STRING);
        DataStreamSource dataGen = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");
        env.execute();
    }
}
