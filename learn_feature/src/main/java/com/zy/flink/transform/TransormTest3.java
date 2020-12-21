package com.zy.flink.transform;

import com.zy.flink.TestUtil;
import com.zy.flink.entity.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: zhangyao
 * @create:2020-12-20 09:27
 * @Description: 滚动聚合算子转换
 **/
public class TransormTest3 {

    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        executionEnvironment.setParallelism(1);

        // 从集合收集数据
        DataStreamSource<SensorReading> sensorReadingDataStreamSource = executionEnvironment.fromCollection(TestUtil.createTestCollection());

        // 根据id keyBy 取最大温度
        SingleOutputStreamOperator<SensorReading> max = sensorReadingDataStreamSource.keyBy("id").maxBy("tmpperature");

        // 输出
        max.print();

        // 执行
        executionEnvironment.execute();

    }
}
