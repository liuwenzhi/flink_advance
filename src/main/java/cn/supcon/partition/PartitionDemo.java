package cn.supcon.partition;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * flink分区相关
 * 通过观察源码可以发现：flink提供了七种分区器和一个自定义
 *
 * Flink 1.17 45课
 */
public class PartitionDemo {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 2.读取数据：socket
        DataStreamSource<String> socketDS = env.socketTextStream("172.16.2.22", 7777);
        // shuffle算子的实现原理：random.nextInt(numberOfChannels)
        // socketDS.shuffle().print();
        // 轮询输出：两个子任务轮班来，适合于数据源source读进来的数据，存在倾斜的情况，比如kafka读入数据时存在数据倾斜
        // socketDS.rebalance().print();
        // 缩放轮询，局部组队，进行轮询比rebalance更高效
        // socketDS.rescale().print();
        // broadcast广播：发送给下游所有的子任务，上边几个分区任务都是1对1的输出，广播是1对多的输出
        // socketDS.broadcast().print();
        // global全局：去不发往第一个子任务，return 0;
        socketDS.global().print();
        // keyby：按指定key去发送，相同key发往同一个子任务
        env.execute();
    }
}
