package com.ssmc.sensorrecord;

import com.ssmc.sensordesc.SensorRecord;

import java.io.IOException;

public interface ISensorStorage {
    /**
     * 写入本地文件
     */
    void writeSensorData(SensorRecord sensorRecord) throws IOException;

    /**
     * 释放资源
     */
    void close() throws IOException;
}
