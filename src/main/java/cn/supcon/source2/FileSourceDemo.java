package cn.supcon.source2;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 将集文件为源端读取数据，注意：wordcount代码里边，读取文件的方法已经被废弃了，从Flink1.13开始，统一从源开始读
 * <p>
 * Flink1.17课程35课
 */
public class FileSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.setParallelism(1);
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("input/word.txt")).build();
        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "filesource").print();
        env.execute();
    }
}
