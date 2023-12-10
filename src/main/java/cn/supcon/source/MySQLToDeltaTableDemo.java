package cn.supcon.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 基于flink-table方式实现mysql源表到delta目的端表
 */
public class MySQLToDeltaTableDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String createSourceDDL = "CREATE TABLE event1( " +
                // "op STRING META FROM 'op' VIRTUAL," +  // 加这个执行不成功，后边再看下
                "id BIGINT," +
                "name STRING," +
                "url STRING," +
                "money BIGINT," +
                "PRIMARY KEY (id) NOT ENFORCED" +
                ")WITH(" +
                "'connector' = 'mysql-cdc'," +
                "'hostname' = '172.16.2.23'," +
                "'port' = '3306'," +
                "'username' = 'root'," +
                "'password' = 'Supcon_21'," +
                "'database-name' = 'data-operation-test'," +
                "'table-name' = 'event'," +
                "'append-mode'='true'" +
                ");";
        tableEnv.executeSql(createSourceDDL);
        tableEnv.executeSql("insert into event1 values (1,'zhangsan','http://123',30000);");
        tableEnv.executeSql("insert into event1 values (2,'lisi','http://1234',40000);");
        tableEnv.executeSql("insert into event1 values (3,'wangwu','http://12345',50000);");
        tableEnv.executeSql("insert into event1 values (4,'zhaoliu','http://123456',60000);");
        // 不创建catalog，Flink会直接使用默认的default_catalog，注意使用这种sql的方式，需要引入flink-json依赖
        String createSinkCatalog = "CREATE CATALOG testDeltaCatalog WITH (" +
                "  'type' = 'delta-catalog'," +
                "  'catalog-type' = 'in-memory'" +
                ");";
        String createSinkDatabase = "CREATE DATABASE IF NOT EXISTS custom_DB;";
        // 创建一个源端表，本质是连接器表
        String createSinkDDL = "CREATE TABLE IF NOT EXISTS table_des (" +
                "    id BIGINT," +
                "    name STRING," +
                "    url STRING," +
                "    money BIGINT" +
                "  ) WITH (" +
                "    'connector' = 'delta'," +
                "    'table-path' = 'hdfs://data62:8020/tmp/table_des'" +
                ");";
        tableEnv.executeSql(createSinkCatalog);
        tableEnv.executeSql(createSinkDatabase);
        tableEnv.executeSql(createSinkDDL);
        tableEnv.executeSql("insert into table_des select * from event1");
        tableEnv.sqlQuery("SELECT * FROM table_des;").execute().print();
    }
}
