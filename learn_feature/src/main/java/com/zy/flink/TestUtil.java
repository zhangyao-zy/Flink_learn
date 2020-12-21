package com.zy.flink;

import com.zy.flink.entity.SensorReading;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author: zhangyao
 * @create:2020-12-19 18:42
 * @Description:
 **/
public class TestUtil {

    public static List<SensorReading> createTestCollection() {
        // 创造集合数据
        List<SensorReading> list = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            list.add(new SensorReading("Sensor" + i, LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli() + ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextDouble(0, 100)));
            list.add(new SensorReading("Sensor" + i, LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli() + ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextDouble(0, 100)));
            list.add(new SensorReading("Sensor" + i, LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli() + ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextDouble(0, 100)));
        }

        // 使用集合收集数据
        return list;
    }
}
