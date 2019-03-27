package com.ssmc.sensordesc;

import android.hardware.SensorEvent;

import java.io.Serializable;
import java.util.HashMap;

import static android.hardware.Sensor.*;

/**
 * 传感器数据的对象封装
 */
public class SensorRecord implements Serializable {

    private static final String TAG = "SensorRecord";
    private String stringType;//对应传感器的名字
    private float values[];//待写入的数据
    private float timeToBeginSecond;//计算从任务开始到现在的用时
    private long timeStamp;//系统时间戳
    private int type;

    private HashMap<Integer, String> mSensorTypeToString = new HashMap<>();

    @Deprecated
    private String currentTime;//当前时间戳

    public SensorRecord(String stringType, float[] values, float timeToBeginSecond, long timeStamp, int type) {
        this.stringType = stringType;
        this.values = values;
        this.timeToBeginSecond = timeToBeginSecond;
        this.timeStamp = timeStamp;
        this.type = type;
    }

    public SensorRecord(SensorEvent event, long timeStamp, float timeToBeginSecond) {
        stringType = getSensorTypeName(event.sensor.getType());
        type = event.sensor.getType();
        values = event.values;
        this.timeStamp = timeStamp;
        this.timeToBeginSecond = timeToBeginSecond;
    }

    private void SetMap(){
        mSensorTypeToString.put(TYPE_ACCELEROMETER,"android.sensor.accelerometer");
        mSensorTypeToString.put(TYPE_GRAVITY,"android.sensor.gravity");
        mSensorTypeToString.put(TYPE_GYROSCOPE,"android.sensor.gyroscope");
        mSensorTypeToString.put(TYPE_LINEAR_ACCELERATION,"android.sensor.linear_acceleration");
        mSensorTypeToString.put(TYPE_ROTATION_VECTOR,"android.sensor.rotation_vector");
        mSensorTypeToString.put(TYPE_MAGNETIC_FIELD,"android.sensor.magnetic_field");
        mSensorTypeToString.put(TYPE_ORIENTATION,"android.sensor.orientation");
        mSensorTypeToString.put(TYPE_PRESSURE,"android.sensor.pressure");
    }

    private String getSensorTypeName(int type){
        SetMap();
        return mSensorTypeToString.get(type);
    }

    @Deprecated
    public SensorRecord(String stringType, float[] values) {
        this.stringType = stringType;
        this.values = values;
    }

    @Deprecated
    public SensorRecord(float timeToBeginSecond, String currentTime,
                        int type, float[] values) {
        this.timeToBeginSecond = timeToBeginSecond;
        this.currentTime = currentTime;
        this.type = type;
        this.values = values;
    }

    public String getStringType() {
        return stringType;
    }

    public float[] getValues() {
        return values;
    }

    public float getTimeToBeginSecond() {
        return timeToBeginSecond;
    }

    public String getCurrentTime() {
        return currentTime;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public int getType() {
        return type;
    }
}
