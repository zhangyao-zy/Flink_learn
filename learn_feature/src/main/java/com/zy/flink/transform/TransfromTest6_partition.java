package com.zy.flink.transform;

import com.zy.flink.entity.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: zhangyao
 * @create:2020-12-21 09:11
 * @Description: 分区操作
 **/
public class TransfromTest6_partition {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 要测试充分区操作,就不能设置并行度为1
        executionEnvironment.setParallelism(4);

        // 读取文件数据
        SingleOutputStreamOperator<SensorReading> dataStreamSource = executionEnvironment.readTextFile("E" +
                ":\\张尧\\idea项目\\tl\\Flink_learn\\learn_feature\\src\\main\\resources" +
                "\\sensorReading.txt").map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0],new Long(split[1]),new Double(split[2]));
        });

        //输出源流
        dataStreamSource.print("input");

        // shuffle
        dataStreamSource.shuffle().print("shuffle");

        // keyBy
        dataStreamSource.keyBy("id").print("keyBy");

        // global
        dataStreamSource.global().print("global");

        // 执行作业
        executionEnvironment.execute();


    }
}
