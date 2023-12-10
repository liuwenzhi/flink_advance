package cn.supcon.source;

import io.delta.flink.sink.DeltaSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;


/**
 * 从kafka源端抽数据到delta数据湖表
 */
public class KafkaToDeltaDemo {
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



        JdbcSource jdbcSource = new JdbcSource();
        DeltaSink<RowData> deltaSink = DeltaSink
                .forRowData(
                        new Path("hdfs://data62:8020/tmp/table_des"),
                        new Configuration(),
                        jdbcSource.getRowType())
                .build();



        // 构造kafka源端对象，包括连接方式，消费者组，topic信息，反序列化，消费offset信息
        /*KafkaSource<String> kafkaSource = KafkaSource.<String>builder().setBootstrapServers("172.16.2.62:9092").setGroupId("supcon").setTopics("topic_test")
                .setValueOnlyDeserializer(new SimpleStringSchema()).setStartingOffsets(OffsetsInitializer.earliest()).build();
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"kafkaSource").sinkTo(deltaSink);*/
        env.execute();
    }
}
