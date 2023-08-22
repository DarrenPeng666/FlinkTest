package org.flink.Source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataGeneratorDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //如果有n个并行度，最大数值设为a
        // 将数值分成n份 a/n 比如最大100 并行度设置为2 每个并行度生成50个
        executionEnvironment.setParallelism(1);

        DataGeneratorSource<String> stringDataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return "Number" + value;
                    }
                },
                10,
                RateLimiterStrategy.perSecond(1),
                Types.STRING
        );

        DataStreamSource<String> stringDataStreamSource = executionEnvironment.fromSource
                (stringDataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");
        stringDataStreamSource.print();
        executionEnvironment.execute();
    }
}
