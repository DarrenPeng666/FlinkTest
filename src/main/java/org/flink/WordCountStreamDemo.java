package org.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStreamDemo {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //2.读取数据 从文件读
        DataStreamSource<String> lineDs = executionEnvironment.readTextFile("input/word.txt");

        //3.处理数据：切分、转换、分组、聚合
        //3.1 切分、转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lineDs.flatMap
                (new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out)
                            throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            // 转换成二元组 (word,1)
                            Tuple2<String, Integer> wordAndOne1 = Tuple2.of(word, 1);
                            out.collect(wordAndOne1);
                        }
                    }
                }).setParallelism(2);

        //3.2分组
        KeyedStream<Tuple2<String, Integer>, String> wordAndOneKs = wordAndOne.keyBy
                (new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                });
        //3.3聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = wordAndOneKs.sum(1);

        sumDS.print();

        executionEnvironment.execute();
    }
}
