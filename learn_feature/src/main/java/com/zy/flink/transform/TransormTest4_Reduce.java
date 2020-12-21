package com.zy.flink.transform;

import com.zy.flink.TestUtil;
import com.zy.flink.entity.SensorReading;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: zhangyao
 * @create:2020-12-20 11:37
 * @Description: reduce操作
 **/
public class TransormTest4_Reduce {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        DataStreamSource<SensorReading> sensorReadingDataStreamSource = executionEnvironment.fromCollection(TestUtil.createTestCollection());

        KeyedStream<SensorReading, Object> keyedStream = sensorReadingDataStreamSource.keyBy(new KeySelector<SensorReading, Object>() {
            @Override
            public Object getKey(SensorReading value) throws Exception {
                return value.getId();
            }
        });

        SingleOutputStreamOperator<SensorReading> reduce = keyedStream.reduce((value1, value2) -> {
            return new SensorReading(value1.getId(), System.currentTimeMillis(), Math.max(value1.getTmpperature(), value2.getTmpperature()));
        });

        reduce.print();

        executionEnvironment.execute();

    }
}
