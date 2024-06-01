package cn.supcon.process;

import cn.supcon.entity.WaterSensor;
import cn.supcon.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

/**
 * TopN
 * 案例需求：实时统计一段时间内的出现次数最多的水位。例如，统计最近10秒钟内出现次数最多的两个水位，并且每5秒钟更新一次。
 *
 * <p>
 * Flink 1.17 94课
 */
public class TopNDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("192.168.190.128", 9999)
                .map(new WaterSensorMapFunction()).assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((elements, ts) -> elements.getTs() * 1000L));

        // 思路一：所有数据放到一起，用hashmap存储。使用全窗口函数实现，并行度被强制设置为1，这时所有的key的数据都会被关联到一块统计
        sensorDS.windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))).process(new MyTopNPAWFunction()).print();

        env.execute();
    }

    /**
     * 自定义全窗口功能类
     * ProcessAllWindowFunction泛型三个类型：输入类型，输出类型，窗口类型
     */
    public static class MyTopNPAWFunction extends ProcessAllWindowFunction<WaterSensor, String, TimeWindow> {

        @Override
        public void process(Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
            // 遍历数据，做累加
            Map<Integer, Integer> vcCountMap = new HashMap<>();
            for (WaterSensor element : elements) {
                Integer vc = element.getVc();
                vcCountMap.put(vc, vcCountMap.getOrDefault(vc, 0) + 1);
            }
            // 借助List列表做排序，这里是根据值进行排序，如果是根据key进行排序可以用TreeMap
            List<Tuple2<Integer, Integer>> datas = new ArrayList<>();
            for (Integer vc : vcCountMap.keySet()) {
                datas.add(Tuple2.of(vc, vcCountMap.get(vc)));
            }
            datas.sort(new Comparator<Tuple2<Integer, Integer>>() {
                @Override
                public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
                    return o2.f1 - o1.f1;
                }
            });
            // 取出count最大的两个vc
            StringBuilder result = new StringBuilder("============================").append("\n");
            for (int i = 0; i < Math.min(2, datas.size()); i++) {
                result.append(datas.get(i).f0 + "," + datas.get(i).f1).append("\n");
            }
            out.collect(result.toString());
        }
    }
}
