package com.zy.flink.source;

import com.zy.flink.entity.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author: zhangyao
 * @create:2020-12-19 17:21
 * @Description: 自定义数据源测试
 **/
public class UDFSourceTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        DataStreamSource<SensorReading> sensorReadingDataStreamSource = executionEnvironment.addSource(new MySensorSource());
        sensorReadingDataStreamSource.print();
        executionEnvironment.execute();
    }


    public static class MySensorSource implements SourceFunction<SensorReading>{

        //定义属性控制数据的生成
        private Boolean running = true;



        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
            //定义传感器集合
            HashMap<String, Double> map = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                map.put("sensor"+i, 60 + ThreadLocalRandom.current().nextGaussian() * 20);
            }
            while (running){
                for (String s : map.keySet()) {
                    sourceContext.collect(new SensorReading(s, LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli(),map.get(s)+ThreadLocalRandom.current().nextGaussian()));
                }
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
