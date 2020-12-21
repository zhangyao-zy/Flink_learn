package com.zy.flink.entity;

/**
 * @author: zhangyao
 * @create:2020-12-19 16:42
 * @Description:
 **/
public class SensorReading {
    private String id;
    private Long timaStamp;
    private Double tmpperature;


    public SensorReading(){

    }

    public SensorReading(String id, Long timaStamp, Double tmpperature) {
        this.id = id;
        this.timaStamp = timaStamp;
        this.tmpperature = tmpperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", timaStamp=" + timaStamp +
                ", tmpperature=" + tmpperature +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimaStamp() {
        return timaStamp;
    }

    public void setTimaStamp(Long timaStamp) {
        this.timaStamp = timaStamp;
    }

    public Double getTmpperature() {
        return tmpperature;
    }

    public void setTmpperature(Double tmpperature) {
        this.tmpperature = tmpperature;
    }
}
