package org.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ConnectTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env.socketTextStream("192.168.198.128", 8888);
        // 在这里可以对数据流进行处理和操作
        stream.print();
        env.execute("Socket Text Stream Example");
    }

}
