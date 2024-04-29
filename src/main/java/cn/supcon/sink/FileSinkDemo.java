package cn.supcon.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;


/**
 * 写入文件的sink算子
 * <p>
 * Flink 1.17 54课
 */
public class FileSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每个目录中，都会有并行度个数的文件在写入
        env.setParallelism(2);
        // 2秒钟关闭一次文件，避免文件出现in progress的情况
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        DataGeneratorSource dataGeneratorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "Number:" + value;
            }
        }, Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(1),
                Types.STRING);
        DataStreamSource dataGen = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");
        // 按照行输出
        FileSink<String> fileSink = FileSink.<String>forRowFormat(new Path("f:/tmp"), new SimpleStringEncoder<>("UTF-8"))
                // 输出一些文件的配置，前缀，后缀名称等
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("abc-").withPartSuffix(".log").build())
                // 指定分桶策略，分桶即是分文件，这里设计根据时间分文件
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH", ZoneId.systemDefault()))
                // 指定文件的滚动策略，根据时间和文件大小两个方面设置，下边设置为10秒或者1M创建一次新文件
                .withRollingPolicy(DefaultRollingPolicy.builder().withRolloverInterval(Duration.ofSeconds(10)).withMaxPartSize(new MemorySize(1024 * 1024)).build())
                .build();
        dataGen.sinkTo(fileSink);
        env.execute();
    }
}
