package cn.supcon.sink;

import cn.supcon.entity.WaterSensor;
import cn.supcon.functions.WaterSensorMapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 写数据到mysql目的端
 * <p>
 * Flink 1.17 59课
 */
public class MySQLSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("172.16.2.23", 7777).map(new WaterSensorMapFunction());
        // sink方法四大参数，sql，预编译对象，执行参数对象（重试，攒批），连接对象
        SinkFunction<WaterSensor> jdbcSink = JdbcSink.sink("insert into ws values(?,?,?)",
                new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                        // 1,2,3对应第一个参数的三个问号
                        preparedStatement.setString(1, waterSensor.getId());
                        preparedStatement.setLong(2, waterSensor.getTs());
                        preparedStatement.setInt(3, waterSensor.getVc());
                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)    // 最大重试次数 3
                        .withBatchSize(100)    // 一个批次达到100条写出去
                        .withBatchIntervalMs(3000)   // 达到3秒写出去，这个条件和上边的100条条件是一个或的关系
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://172.16.2.23:3306/ceshi?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                        .withUsername("root")
                        .withPassword("Supcon_21")
                        .withConnectionCheckTimeoutSeconds(60)   // 两次重试之间的时长
                        .build());
        // JdbcSink还没迁移过来，暂时不能用sinkTo，只能用sink
        sensorDS.addSink(jdbcSink);
        env.execute();
    }
}
