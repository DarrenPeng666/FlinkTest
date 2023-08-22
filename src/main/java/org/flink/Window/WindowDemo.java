package org.flink.Window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.
                getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        DataGeneratorSource<String> stringDataGeneratorSource = new DataGeneratorSource<>(
                value -> "Number" + value,
                Long.MAX_VALUE, //实现无界流
                RateLimiterStrategy.perSecond(3), //每秒钟产生多少条数据
                Types.STRING
        );

        DataStreamSource<String> stringDataStreamSource = executionEnvironment.fromSource
                (stringDataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");


        KeyedStream<String, Integer> stringIntegerKeyedStream = stringDataStreamSource.
                keyBy(value -> Integer.valueOf(value));

//        //基于时间 滚动窗口
//        stringIntegerKeyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
//
//        //滑动窗口，窗口长度10s，滑动步长2s
//        stringIntegerKeyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(2)));
//
//        //会话窗口，超时间隔5s
//        stringIntegerKeyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));
//
//        //基于计数的 滚动窗口 窗口长度=5个元素
//        stringIntegerKeyedStream.countWindow(5);
//
//        //滑动窗口 窗口长度=5个元素， 滑动步长=2个元素
//        stringIntegerKeyedStream.countWindow(5,2);

        stringIntegerKeyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        executionEnvironment.execute();
    }
}
