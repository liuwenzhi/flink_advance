package cn.supcon.source;

import cn.supcon.entity.Event;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 数据写入mysql目的端
 * 参考代码：
 * https://zhuanlan.zhihu.com/p/644300210
 */
public class MySQLSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // source
        DataStream<Event> userDS = env.fromElements(new Event("zhangsan1", "http://123", 1000L));
        String sql = "INSERT INTO `event` (`id`,`name`, `url`, `money`) VALUES (null, ?, ?, ?);";
        String driverName = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://172.16.2.23:3306/data-operation-test?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
        String username = "root";
        String password = "Supcon_21";
        userDS.addSink(JdbcSink.sink(sql, (ps, value) -> {
            ps.setString(1, value.getName());
            ps.setString(2, value.getUrl());
            ps.setLong(3, value.getMoney());
        }, new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withDriverName(driverName).withUrl(url).withUsername(username).withPassword(password).build()));

        // execute
        env.execute();
    }
}
