package org.flink.Process;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.flink.Bean.WaterSensor;
import org.flink.Function.WaterSensorMapFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;

public class TopNDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> streamOperator = executionEnvironment
                .socketTextStream("192.168.198.128", 8888)
//                .fromElements("s1,2,5","s1,4,5","s2,5,5","s1,13,8")
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> element.ts * 1000L)
                );

        streamOperator.windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new ProcessAllWindowFunction<WaterSensor, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<WaterSensor, String, TimeWindow>.Context context,
                                        Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        HashMap<Integer, Integer> map = new HashMap<>();
                        for (WaterSensor element : elements) {
                            if (map.containsKey(element.getVc())) {
                                // 如果不是第一条数据
                                map.put(element.getVc(), map.get(element.getVc()) + 1);
                            } else {
                                // 如果是第一条数据
                                map.put(element.getVc(), 1);
                            }
                        }

                        //2对count数值进行排序,去除count最大的2个vc
                        ArrayList<Tuple2<Integer, Integer>> tuple2s = new ArrayList<>();
                        for (Integer i : map.keySet()) {
                            tuple2s.add(Tuple2.of(i, map.get(i)));
                        }
                        tuple2s.sort((o1, o2) -> o2.f1 - o1.f1);

                        // 2. 取TopN
                        StringBuilder outStr = new StringBuilder();

                        outStr.append("================================\n");
                        // List中元素的个数 和 2 取最小值
                        for (int i = 0; i < Math.min(2, tuple2s.size()); i++) {
                            Tuple2<Integer, Integer> vcCount = tuple2s.get(i);
                            outStr.append("Top" + (i + 1) + "\n");
                            outStr.append("vc=" + vcCount.f0 + "\n");
                            outStr.append("count=" + vcCount.f1 + "\n");
                            outStr.append("================================\n");
                        }
                        out.collect(outStr.toString());
                    }
                }).print();


        executionEnvironment.execute();
    }
}
