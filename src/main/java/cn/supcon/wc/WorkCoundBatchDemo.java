package cn.supcon.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.util.Collector;

/**
 * 从input目录word文件读取数据，以批处理方式统计单词数量
 */
public class WorkCoundBatchDemo {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.读取数据，相对路径，从根目录读取
        DataSource<String> lineDS = env.readTextFile("input/word.txt");

        // 3.切分，转换（word，1）
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                // 3.1 按照空格切分单词
                String[] words = value.split(" ");
                // 3.2 拆分单词后转成元组
                for (String word : words) {
                    Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);
                    // 3.3 使用Collector向下游发数据
                    out.collect(wordTuple2);
                }
            }
        });

        // 4.按word分组，根据二元组的元素位置传参（位置编号）
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGroupBy = wordAndOne.groupBy(0);

        // 5.分组组内聚合
        AggregateOperator<Tuple2<String, Integer>> sum = wordAndOneGroupBy.sum(1);

        // 6.输出
        sum.print();
    }
}
