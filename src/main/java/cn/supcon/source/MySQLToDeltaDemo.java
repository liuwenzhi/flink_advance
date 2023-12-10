package cn.supcon.source;

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
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;

/**
 * 从mysql源端同步数据到delta目的端
 */
public class MySQLToDeltaDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 不创建catalog，Flink会直接使用默认的default_catalog，注意使用这种sql的方式，需要引入flink-json依赖
        String createCatalog = "CREATE CATALOG testDeltaCatalog WITH (" +
                "  'type' = 'delta-catalog'," +
                "  'catalog-type' = 'in-memory'" +
                ");";
        String createDatabase = "CREATE DATABASE IF NOT EXISTS custom_DB;";
        // 创建一个源端表，本质是连接器表
        String createSourceDDL = "CREATE TABLE IF NOT EXISTS table_des (" +
                "    name STRING," +
                "    url STRING," +
                "    money BIGINT" +
                "  ) WITH (" +
                "    'connector' = 'delta'," +
                "    'table-path' = 'hdfs://data62:8020/tmp/table_des'" +
                ");";
        tableEnv.executeSql(createCatalog);
        tableEnv.executeSql("USE CATALOG testDeltaCatalog;");
        tableEnv.executeSql(createDatabase);
        tableEnv.executeSql("USE custom_DB;");
        tableEnv.executeSql(createSourceDDL);

        // 基于表的自增主键执行checkpoint相关操作，5秒钟执行一次
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(5000);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://data62:8020/tmp/checkpoints"));
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        JdbcSource jdbcSource = new JdbcSource();
        DataStreamSource<JSONObject> source = env.addSource(jdbcSource);
        SingleOutputStreamOperator<RowData> mapStream = source.map(new MapFunction<JSONObject, RowData>() {
            @Override
            public RowData map(JSONObject jsonObject) throws Exception {
                //todo RowData internal = RowUtil.toInternal(value,jdbcSource.getRowType());
                // return internal;
                return null;
            }
        });

        DeltaSink<RowData> deltaSink = DeltaSink
                .forRowData(
                        new Path("hdfs://data62:8020/tmp/table_des"),
                        new Configuration(),
                        jdbcSource.getRowType())
                .build();

        env.execute();
    }
}
