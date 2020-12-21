package com.zy.flink.sink;

import com.zy.flink.TestUtil;
import com.zy.flink.entity.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author: zhangyao
 * @create:2020-12-21 10:54
 * @Description: 从文件中读取到数据输出到redis
 **/
public class RedisSinkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<SensorReading> sensorReadingDataStreamSource = executionEnvironment.fromCollection(TestUtil.createTestCollection());

        // 创建jedis配置环境
        FlinkJedisPoolConfig flinkJedisPoolConfig =
                new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();
        DataStreamSink test = sensorReadingDataStreamSource.addSink(new RedisSink(flinkJedisPoolConfig,
                new RedisMapper<SensorReading>() {
                    // 创建执行方法的描述
                    @Override
                    public RedisCommandDescription getCommandDescription() {
                        return new RedisCommandDescription(RedisCommand.HSET, "test");
                    }

                    @Override
                    public String getKeyFromData(SensorReading sensorReading) {
                        return sensorReading.getId();
                    }

                    @Override
                    public String getValueFromData(SensorReading sensorReading) {
                        return sensorReading.getTmpperature().toString();
                    }
                }));

        executionEnvironment.execute();
    }
}
