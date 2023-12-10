package cn.supcon.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 测试delta数据湖相关操作
 * 直接基于delta-connector的方式，创建两张表（表文件内容保存在hdfs目录下），先创建一张表并向这张表里边插入数据，
 * 接下来根据这张表创建第二张表，把这张表里边数据导入到第二张表中
 * 参考材料：https://github.com/delta-io/connectors/tree/master/flink
 */
public class DeltaTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 不创建catalog，Flink会直接使用默认的default_catalog，注意使用这种sql的方式，需要引入flink-json依赖
        String createCatalog = "CREATE CATALOG testDeltaCatalog WITH (" +
                "  'type' = 'delta-catalog'," +
                "  'catalog-type' = 'in-memory'" +
                ");";
        String createDatabase = "CREATE DATABASE IF NOT EXISTS custom_DB;";
        // 创建一个源端表，本质是连接器表
        String createSourceDDL = "CREATE TABLE IF NOT EXISTS table1 (" +
                "    id BIGINT," +
                "    name STRING," +
                "    address STRING," +
                "    update_time STRING" +
                "  ) WITH (" +
                "    'connector' = 'delta'," +
                "    'table-path' = 'hdfs://data62:8020/tmp/table1'" +
                ");";
        // 通过like语句创建一张目的端表，不指定具体的字段设置
        String createSinkDDL = "CREATE TABLE IF NOT EXISTS table2 " +
                "  WITH (" +
                "    'connector' = 'delta'," +
                "    'table-path' = 'hdfs://data62:8020/tmp/table2'" +
                ") LIKE table1;";
        // 自定义插入源表数据，注意executeSql方法一次只能执行一条sql语句，这里定义四条插入sql语句
        String insertSourceSQL1 = "insert into table1 values (1,'zhangsan','abcdefg','2023-07-21 10:33:24');";
        String insertSourceSQL2 = "insert into table1 values (2,'lisi','erwq34234ersdf','2023-06-21 11:12:24');";
        String insertSourceSQL3 = "insert into table1 values (3,'wangwu','zcxvwe213erf','2023-07-13 15:23:42');";
        String insertSourceSQL4 = "insert into table1 values (4,'zhaoliu','23erdfzcxsd','2023-08-29 10:01:24');";
        // 自定义从源表插入数据到目的端表sql
        String insertSinkSQL = "insert into table2 select * from table1";
        tableEnv.executeSql(createCatalog);
        tableEnv.executeSql("USE CATALOG testDeltaCatalog;");
        tableEnv.executeSql(createDatabase);
        tableEnv.executeSql("USE custom_DB;");
        tableEnv.executeSql(createSourceDDL);
        tableEnv.executeSql(createSinkDDL);
        tableEnv.executeSql(insertSourceSQL1);
        tableEnv.executeSql(insertSourceSQL2);
        tableEnv.executeSql(insertSourceSQL3);
        tableEnv.executeSql(insertSourceSQL4);
        tableEnv.executeSql(insertSinkSQL);
        // 流式输出，只有建表之后，才能直接将表名放到字符串里边直接执行输出
        tableEnv.sqlQuery("SELECT * FROM table1 /*+ OPTIONS('mode' = 'streaming') */;").execute().print();
        // 跑批方式输出，只有执行建表操作之后，才能将表名写到sql语句中执行查询
        // tableEnv.sqlQuery("SELECT * FROM table2;").execute().print();
        // 转流的打印没有效果
        /*Table table1 = tableEnv.sqlQuery("SELECT * FROM table1;");
        tableEnv.toDataStream(table1).print("table1");*/
    }
}
