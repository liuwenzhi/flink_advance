package cn.supcon.source;

import cn.supcon.utils.RowUtil;
import com.alibaba.fastjson.JSONObject;
import io.delta.flink.sink.DeltaSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

/**
 * 从mysql源端同步数据到delta目的端
 */
public class MySQLToDeltaDemo {
    public static void main(String[] args) throws Exception{
        org.apache.flink.configuration.Configuration conf = new  org.apache.flink.configuration.Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 基于表的自增主键执行checkpoint相关操作，5秒钟执行一次
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(5000);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://data62:8020/tmp/checkpoints"));
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 自定义jdbc source
        MySqlJdbcSource jdbcSource = new MySqlJdbcSource();
        DataStreamSource<JSONObject> source = env.addSource(jdbcSource);

        // 自定义转换算子
        SingleOutputStreamOperator<RowData> mapStream = source.map(new MapFunction<JSONObject, RowData>() {
            @Override
            public RowData map(JSONObject jsonObject) throws Exception {
                RowData internal = RowUtil.toInternal(jsonObject,jdbcSource.getRowType());
                return internal;
            }
        });

        // 自定义目的端算子
        DeltaSink<RowData> deltaSink = DeltaSink
                .forRowData(
                        new Path("hdfs://data62:8020/user/hive/warehouse/stg.db/test_1213"),
                        new org.apache.hadoop.conf.Configuration(),
                        jdbcSource.getRowType())
                .build();
        mapStream.sinkTo(deltaSink);
        env.execute();
    }
}
