package org.flink.State;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.flink.Bean.WaterSensor;
import org.flink.Function.WaterSensorMapFunction;

import java.time.Duration;
import java.util.Map;


//TODO 案例需求：统计每种传感器每种水位值出现的次数。
public class KeyedMapStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> streamOperator = env.socketTextStream("192.168.198.128", 8888)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((element, ts) -> element.getTs() * 1000L));


        streamOperator.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        }).process(new KeyedProcessFunction<String, WaterSensor, String>() {
            MapState<Integer, Integer> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("mapState", Types.INT, Types.INT));
            }

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx,
                                       Collector<String> out) throws Exception {
                //如果水位线已经存在map中
                if (mapState.contains(value.getVc())) {
                    Integer count = mapState.get(value.getVc());
                    mapState.put(value.getVc(), ++count);
                }
                //水位线不存在map中
                else {
                    mapState.put(value.getVc(), 1);
                }

                for (Map.Entry<Integer, Integer> entry : mapState.entries()) {
                    StringBuilder outStr = new StringBuilder();
                    outStr.append("======================================\n");
                    outStr.append("传感器id为" + value.getId() + "\n");
                    for (Map.Entry<Integer, Integer> vcCount : mapState.entries()) {
                        outStr.append(vcCount.toString() + "\n");
                    }
                    outStr.append("======================================\n");

                    out.collect(outStr.toString());
                }

            }
        }).print();

        env.execute();


    }

}
