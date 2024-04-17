package cn.supcon.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 富函数类
 * <p>
 * Flink1.17 44课
 */
public class RichFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4);
        SingleOutputStreamOperator<Integer> map = source.map(new RichMapFunction<Integer, Integer>() {

            // 富函数中的生命周期函数,open函数在map函数之前被调用
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 富函数类提供的运行时上下文对象，获取运行时的环境，信息，比如任务编号名称等
                RuntimeContext runtimeContext = getRuntimeContext();
                int indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();
                String taskNameWithSubtasks = runtimeContext.getTaskNameWithSubtasks();
                System.out.println("open函数开始执行,子任务id:" + indexOfThisSubtask + ",子任务名称:" + taskNameWithSubtasks);
            }

            // 富函数中的生命周期函数，close函数在map函数之后被调用
            @Override
            public void close() throws Exception {
                super.close();
                RuntimeContext runtimeContext = getRuntimeContext();
                int indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();
                String taskNameWithSubtasks = runtimeContext.getTaskNameWithSubtasks();
                System.out.println("close函数开始执行,子任务id:" + indexOfThisSubtask + ",子任务名称:" + taskNameWithSubtasks);
            }

            @Override
            public Integer map(Integer value) throws Exception {
                return value + 1;
            }
        });
        map.print();
        env.execute();
    }
}
