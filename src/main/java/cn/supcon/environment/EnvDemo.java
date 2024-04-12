package cn.supcon.environment;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * Flink1.17 33课代码
 */
public class EnvDemo {
    public static void main(String[] args) throws Exception {
        // 第一种，创建本地运行环境，即idea的运行环境，这个时候，会创建一个mini集群，正常开发不会用到这种
        // StreamExecutionEnvironment.createLocalEnvironment();
        // 创建一个远程的集群环境，指定服务器名称，端口号，正常开发也不会用到这种
        // StreamExecutionEnvironment.createRemoteEnvironment("data62",8081,"/abc/123");
        /*直接获取环境，这个方法底层逻辑会先去获取一个远程的环境（程序运行在服务器上），如果没有存在的远程环境，就按照默认配置获取本地环境（程序运行在本地idea中）
         * 开发过程中可以直接用get这个，不需要区分本地环境和远程环境*/
        // StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT, "8082");
        /*Configuration对象可以修改运行环境具体参数，通过getExecutionEnvironment方法加入了conf参数可以在本地访问flink界面，
        不加自定义的conf，该方法按照默认的configuration对象启动环境，本地访问不了flink界面*/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 通过env设置流或者批运行环境
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.socketTextStream("172.16.2.23", 7777).flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
            String[] words = value.split(" ");
            for (String word : words) {
                Tuple2<String, Integer> wordAndOne = Tuple2.of(word, 1);
                // 生成的对象用采集器向下游发送数据
                out.collect(wordAndOne);
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy((Tuple2<String, Integer> value) -> value.f0).sum(1).print();
        env.execute();
    }
}
