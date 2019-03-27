package com.ssmc.sensorrecord;

import android.app.Service;
import android.content.Intent;
import android.hardware.Sensor;
import android.hardware.SensorEvent;
import android.hardware.SensorEventListener;
import android.hardware.SensorManager;
import android.os.Binder;
import android.os.Build;
import android.os.IBinder;
import android.support.annotation.RequiresApi;

import com.ssmc.sensordesc.SensorRecord;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * 读取传感器数据并异步存储到本地
 */
public class WearSensorRecordService extends Service {

    private SensorManager mSensorManager;
    private MobileTransfer mMobileTransfer;
    private SensorEventListener mSensorEventListener = new MySensorEventListener();

    private float timeBeginMillis;

    @Override
    public void onCreate() {
        //1.获取SensorManager实例
        mSensorManager = (SensorManager) this.getSystemService(Service.SENSOR_SERVICE);
        mMobileTransfer = new MobileTransfer(getApplicationContext());
        mMobileTransfer.connect();
        super.onCreate();
    }

    @Override
    public IBinder onBind(Intent intent) {
        return new SensorRecordBinder();
    }

    /**
     * 监听器，监听传感器返回的数据
     * {@link SensorEventListener}
     */
    public class MySensorEventListener implements SensorEventListener {

        @Override
        public void onSensorChanged(SensorEvent event) {
            long currentTime = System.currentTimeMillis();
            float timeToBeginSecond = (currentTime - timeBeginMillis) / 1000.f;
            SensorRecord sensorRecord = new SensorRecord(event,currentTime,timeToBeginSecond);
            mMobileTransfer.sendSensorData(sensorRecord);
        }

        @Override
        public void onAccuracyChanged(Sensor sensor, int accuracy) {

        }
    }

    /**
     * Binder实现类
     */
    public class SensorRecordBinder extends Binder {

        /**
         * 注册待监听的传感器接收器，将需要监听的传感器的常量{@link Sensor}放入集合传入进来，
         * 举例来说，如果想要监听加速度，可以把常量{@link Sensor#TYPE_ACCELEROMETER}作为
         * sensorTypes中的一个变量传入。如果无法监听某个传感器，将会把该传感器的常量保存在
         * 一个集合中并返回这个常量集合。
         *
         * @param sensorTypes {@link Sensor}中代表传感器的常量如{@link Sensor#TYPE_ACCELEROMETER}
         */
        public void start(Set<Integer> sensorTypes) {
            //2.注册对应传感器的监听器
            registerListener(sensorTypes);
            timeBeginMillis = System.currentTimeMillis();
        }

        public void stop() {
            mMobileTransfer.disConnect();
            timeBeginMillis = 0;
            mSensorManager.unregisterListener(mSensorEventListener);
        }

        private void registerListener(Set<Integer> sensorTypes){
            for (int sensorType : sensorTypes) {
                if (mSensorManager.getDefaultSensor(sensorType) != null) {
                    mSensorManager.registerListener(mSensorEventListener,
                            mSensorManager.getDefaultSensor(sensorType),
                            SensorManager.SENSOR_DELAY_NORMAL);
                }
            }
        }
    }
}
