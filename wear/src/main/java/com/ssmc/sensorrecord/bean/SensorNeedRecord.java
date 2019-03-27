package com.ssmc.sensorrecord.bean;

public class SensorNeedRecord {
    public int sensorType;
    public String fileName;

    public SensorNeedRecord(String fileName, int sensorType) {
        this.sensorType = sensorType;
        this.fileName = fileName;
    }
}
