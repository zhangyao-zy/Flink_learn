package com.zy.flink.transform;

import com.zy.flink.TestUtil;
import com.zy.flink.entity.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: zhangyao
 * @create:2020-12-19 17:55
 * @Description:
 **/
public class TransormTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);


        DataStreamSource<SensorReading> sensorReadingDataStreamSource = executionEnvironment.fromCollection(TestUtil.createTestCollection());
        // map转换 返回sensorReading的sensorId
        SingleOutputStreamOperator<Object> map = sensorReadingDataStreamSource.map(new MapFunction<SensorReading, Object>() {
            @Override
            public Object map(SensorReading sensorReading) throws Exception {
                return sensorReading.getId();
            }
        });

        // flatMap转换 将各个属性拆分输出
        SingleOutputStreamOperator<Object> flatMap = sensorReadingDataStreamSource.flatMap(new FlatMapFunction<SensorReading, Object>() {
            @Override
            public void flatMap(SensorReading sensorReading, Collector<Object> collector) throws Exception {
                String[] split = sensorReading.toString().split(", ");
                for (String s : split) {
                    collector.collect(s);
                }
            }
        });


        // filter 过滤转换
        SingleOutputStreamOperator<SensorReading> filter = sensorReadingDataStreamSource.filter(new FilterFunction<SensorReading>() {
            @Override
            public boolean filter(SensorReading sensorReading) throws Exception {
                return sensorReading.getId().equals("Sensor2");

            }
        });

        // 输出map转换后的数据
        map.print("map");

        // 输出flatmao转换后的数据
        flatMap.print("flatMap");

        // 输出filter转换后的数据
        filter.print("filter");

        executionEnvironment.execute();
    }
}
