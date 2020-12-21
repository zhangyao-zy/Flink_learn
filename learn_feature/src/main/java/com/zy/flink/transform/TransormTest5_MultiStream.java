package com.zy.flink.transform;

import com.zy.flink.entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.Collections;

/**
 * @author: zhangyao
 * @create:2020-12-20 11:54
 * @Description:
 **/
public class TransormTest5_MultiStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        DataStreamSource<String> dataStreamSource = executionEnvironment.readTextFile("E:\\张尧\\idea项目\\tl\\Flink_learn\\learn_feature\\src\\main\\resources\\sensorReading.txt");

        SingleOutputStreamOperator<SensorReading> map = dataStreamSource.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) {
                String[] split = value.split(",");
                return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
            }
        });

        SplitStream<SensorReading> split = map.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return sensorReading.getTmpperature() > 36 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        DataStream<SensorReading> high = split.select("high");
        DataStream<SensorReading> low = split.select("low");
        high.print("high");
        low.print("low");

        map.print("all");

        executionEnvironment.execute();


    }
}
