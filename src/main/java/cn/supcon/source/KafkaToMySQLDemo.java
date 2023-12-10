package cn.supcon.source;

import cn.supcon.entity.Event;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 * 验证从kafka源端到mysql目的端（运行失败）
 * 参考代码：
 * https://blog.csdn.net/javahelpyou/article/details/124894302
 */
public class KafkaToMySQLDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.2.62:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "earliest");
        DataStreamSource<String> stream = env.addSource(new
                FlinkKafkaConsumer<String>(
                "topic_test",
                new SimpleStringSchema(),
                properties
        ));

        SingleOutputStreamOperator<Event> flatMap = stream.flatMap(new FlatMapFunction<String, Event>() {
            @Override
            public void flatMap(String s, Collector<Event> collector) throws Exception {
                collector.collect(JSON.parseObject(s, Event.class));
            }
        });

        JdbcStatementBuilder<Event> jdbcStatementBuilder = new JdbcStatementBuilder<Event>() {
            @Override
            public void accept(PreparedStatement preparedStatement, Event event) throws SQLException {
                preparedStatement.setString(1, event.getName());
                preparedStatement.setString(2, event.getUrl());
                preparedStatement.setLong(3,event.getMoney());
            }
        };
        JdbcConnectionOptions jdbcConnectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl("jdbc:mysql://172.16.2.23:3306/data-operation-test")
                // 对于 MySQL 5.7，用"com.mysql.jdbc.Driver"
                .withDriverName("com.mysql.jdbc.Driver")
                .withUsername("root")
                .withPassword("Supcon_21")
                .build();

        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();
        flatMap.addSink(JdbcSink.sink(" insert into event (user,url,timestamp)values (?,?,?)", jdbcStatementBuilder, executionOptions, jdbcConnectionOptions));

        env.execute();
    }

}
