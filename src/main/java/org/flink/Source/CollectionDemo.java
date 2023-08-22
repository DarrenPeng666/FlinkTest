package org.flink.Source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class CollectionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从集合读取数据
        DataStreamSource<Integer> integerDataStreamSource = executionEnvironment.fromCollection
                (Arrays.asList(1, 2, 3, 4));

        integerDataStreamSource.print();


        executionEnvironment.execute();
    }
}
