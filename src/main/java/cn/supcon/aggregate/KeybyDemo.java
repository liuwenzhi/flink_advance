package cn.supcon.aggregate;

import cn.supcon.entity.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * key by代码样例
 * keyBys算子返回的是一个KeyedStream，键控流，keyby不是转换算子，只是对数据进行重分区，不能设置并行度
 * keyby 分组与分区的关系：
 * （1） keyby是对数据分组，保证相同的key的数据在同一个分区
 * （2） 分组：一个子任务，可以理解为一个分区（代码中可以通过修改并行度查看，并行度大于1的时候，打印结果可以看到相同的数据对应同一个线程号）
 * 这里可以这么来理解：比如有20个学生在一个教室，分成四个组，每组5个人，分组就是keyby算子做的事情，教室就相当于是分区（物理上的资源，相当于子任务，并行度），同组的同学在一个分区里边
 * 或者可以这么来理解，原始数据：aabbcc，可以分成a,b,c三个分组，但是实际只有两个分区，0和1（并行度设置为2），这个时候，会有一个分区存放多个分组，比如0号分区存放a，b两个组，keyby算子
 * 是保证同一个分组的数据，存在于同一个分区里边。如果此时包括4个分区，0,1,2,3，那么就可能会有一个分区是空着的。
 *
 * Flink1.17课程41课
 */
public class KeybyDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        DataStreamSource<WaterSensor> sensorDS = env.fromElements(new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11), new WaterSensor("s2", 2L, 2), new WaterSensor("s3", 3L, 3));
        // 按照key来进行分组
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });
        sensorKS.print();
        env.execute();
    }
}
