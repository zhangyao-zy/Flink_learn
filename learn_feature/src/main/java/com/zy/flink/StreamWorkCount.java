package com.zy.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: zhangyao
 * @create:2020-12-18 20:30
 * @Description:
 **/
public class StreamWorkCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        String inputPath = "E:\\张尧\\idea项目\\FlinkTutorial\\src\\main\\resources\\word.txt";
//        DataStreamSource<String> dataStreamSource = executionEnvironment.readTextFile(inputPath);

        DataStreamSource<String> dataStreamSource = executionEnvironment.socketTextStream("192.168.164.205", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = dataStreamSource.flatMap(new WorkCount.MyFlagMapFunction()).keyBy(0).sum(1);
        sum.print();

        executionEnvironment.execute();
    }
}
