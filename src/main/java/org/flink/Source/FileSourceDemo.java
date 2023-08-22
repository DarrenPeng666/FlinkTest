package org.flink.Source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度=1
        executionEnvironment.setParallelism(2);

        //读取文件
        FileSource<String> build = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(), new Path("input/word.txt")
        ).build();
        executionEnvironment.fromSource(build, WatermarkStrategy.noWatermarks(), "file")
                .print();
        executionEnvironment.execute();

    }
}
