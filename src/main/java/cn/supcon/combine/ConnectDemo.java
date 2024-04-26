package cn.supcon.combine;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * connect合流方案：一次可以合并多条流，流的数据类型可以不一致，不用进行类型强转
 * 缺点：connect算子一次只能连接两个流，连接之后可以调用多个算子，union算子可以连接多个
 * 一国两制，同床异梦，哈哈哈哈，这节必须好好掌握
 * <p>
 * Flink 1.17 50课
 */
public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 集合数据模拟合流
        /*DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3);
        DataStreamSource<Integer> source2 = env.fromElements(11, 22, 33);
        DataStreamSource<String> source3 = env.fromElements("111", "222", "333");*/
        // 通过socket读取两条流，执行无界流的合并
        SingleOutputStreamOperator<Integer> source1 = env.socketTextStream("172.16.2.22", 7777).map(i -> Integer.parseInt(i));
        DataStreamSource<String> source3 = env.socketTextStream("172.16.2.23", 7777);
        // 只能connect一个流，不能多个，connect返回的结果不再试DataStream，而是一个Connectedtream，connect流也有自己的算子
        ConnectedStreams<Integer, String> connect = source1.connect(source3);
        // connect的map算子，也是一个分开处理的算子，两个流各玩各的
        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "整型内容输出：" + value;
            }

            @Override
            public String map2(String value) throws Exception {
                return "字符串类型输出：" + value;
            }
        });
        map.print();
        env.execute();
    }
}
