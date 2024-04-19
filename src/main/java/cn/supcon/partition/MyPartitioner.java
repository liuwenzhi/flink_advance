package cn.supcon.partition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * 自定义分区器，采用取余方式，均匀分配
 * <p>
 * Flink 1.17 46课
 */
public class MyPartitioner implements Partitioner<String> {

    /**
     * key：入参的数值
     * numPartitions；下游算子的分区数
     */
    @Override
    public int partition(String key, int numPartitions) {
        return Integer.parseInt(key) % numPartitions;
    }
}
