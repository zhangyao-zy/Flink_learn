package com.zy.flink.source;

import com.zy.flink.entity.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author: zhangyao
 * @create:2020-12-19 16:42
 * @Description:
 **/
public class CollectionSourceTest {

    public static void main(String[] args) throws Exception {

        // 创建流处理执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度1
        executionEnvironment.setParallelism(1);

        // 创造集合数据
        List<SensorReading> list = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            list.add(new SensorReading("Sensor" + i, LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli(), ThreadLocalRandom.current().nextDouble(35, 40)));
        }
        // 从集合中收集数据
        DataStreamSource<SensorReading> sensorReadingDataStreamSource = executionEnvironment.fromCollection(list);
        // 打印集合数据
        sensorReadingDataStreamSource.print("sensor");

        // 从元素中收集数据
        DataStreamSource<Integer> integerDataStreamSource = executionEnvironment.fromElements(1, 2, 3, 4, 56, 7);
        // 打印从元素中收集到数据
        integerDataStreamSource.print("element");

        // 执行Flink程序
        executionEnvironment.execute();

    }
}
