package org.flink.Partition;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4);

        //随机分区 random.nextInt()下游算子并行度
        stream.shuffle().print();

//        //轮询分区
//        stream.rebalance().print();

//        //缩放轮询 实现轮询 局部组队 比rebalance更加高效
//        stream.rescale();

//        //广播 发送给下游所有的子任务
//        stream.broadcast();

//        //全局分区也是一种特殊的分区方式。这种做法非常极端，通过调用.global()方法，
//        // 会将所有的输入流数据都发送到下游算子的第一个并行子任务中去。
//        stream.global();

//        //keyBy分区 按照key组队进行发送下游的子任务
//        stream.keyBy(new KeySelector<Integer, Integer>() {
//            @Override
//                public Integer getKey(Integer value) throws Exception {
//                return value;
//            }
//        });

        env.execute();
    }
}
