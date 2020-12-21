package com.zy.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * @author: zhangyao
 * @create:2020-12-21 10:30
 * @Description: 从kafka中读取数据,写入到kafka
 **/
public class KafkaSink {

    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        // 从kafka获取消息
        // 创建kafka配置信息
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.164.205:9092");

        // 消费消息
        DataStreamSource<String> sourcetest =
                executionEnvironment.addSource(new FlinkKafkaConsumer<String>("sourcetest",
                new SimpleStringSchema(), properties));


        // 写入kafka
        DataStreamSink sinktest = sourcetest.addSink(new FlinkKafkaProducer("192.168.164.205:9092", "sinktest", new SimpleStringSchema()));

        // 执行作业
        executionEnvironment.execute();

    }
}
