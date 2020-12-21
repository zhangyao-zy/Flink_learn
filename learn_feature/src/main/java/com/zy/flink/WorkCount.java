package com.zy.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author: zhangyao
 * @create:2020-12-18 17:01
 * @Description: 简单上手Flink 分词程序
 **/
public class WorkCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        String inputPath = "E:\\张尧\\idea项目\\FlinkTutorial\\src\\main\\resources\\word.txt";
        DataSource<String> wordDataSet = executionEnvironment.readTextFile(inputPath);

        AggregateOperator<Tuple2<String, Integer>> sum = wordDataSet.flatMap(new MyFlagMapFunction()).groupBy(0).sum(1);

        sum.print();
    }

    public static class MyFlagMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] s1 = s.split(" ");
            for (String s2 : s1) {
                collector.collect(new Tuple2<>(s2, 1));
            }
        }
    }


}
