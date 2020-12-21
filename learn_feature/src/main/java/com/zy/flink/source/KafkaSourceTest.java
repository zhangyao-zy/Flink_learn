package com.zy.flink.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author: zhangyao
 * @create:2020-12-21 10:04
 * @Description: 测试kafka数据源
 **/
public class KafkaSourceTest {

    public static void main(String[] args) throws Exception {

        // 创建kafka连接配置信息
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.164.205:9092");
//        properties.setProperty("group.id", "")

        // 创建流处理执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        // 从kafka从获取数据
        DataStreamSource<String> dataStreamSource = executionEnvironment.addSource(new FlinkKafkaConsumer<String>("sourcetest",
                new SimpleStringSchema(), properties));

        dataStreamSource.print();

        executionEnvironment.execute();
    }
}
