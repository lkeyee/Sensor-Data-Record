package com.ssmc.glass;

import android.app.Service;
import android.content.ComponentName;
import android.content.Intent;
import android.content.ServiceConnection;
import android.hardware.Sensor;
import android.hardware.SensorEvent;
import android.hardware.SensorEventListener;
import android.hardware.SensorManager;
import android.os.Binder;
import android.os.IBinder;
import android.util.Log;

import com.ssmc.sensordesc.SensorRecord;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 读取传感器数据并异步存储到本地
 */
public class GlassSensorRecordService extends Service {

    private static String TAG = "GlassGlassSensorRecordService";

    List<Integer> typeList = new ArrayList<>(Arrays.asList(
            Sensor.TYPE_GRAVITY, Sensor.TYPE_GYROSCOPE, Sensor.TYPE_LINEAR_ACCELERATION,
            Sensor.TYPE_ROTATION_VECTOR, Sensor.TYPE_MAGNETIC_FIELD, Sensor.TYPE_ORIENTATION,Sensor.TYPE_PRESSURE)
    );

    private SensorManager mSensorManager;
    private SensorEventListener mSensorEventListener;
    private SensorDataWriter mSensorDataWriter;
    private long startTime;

    private GlassSensorRecordService.SensorRecordBinder mService;
    //通过ServiceConnection来监听与service的连接
    private ServiceConnection mConnection = new ServiceConnection() {
        @Override
        public void onServiceConnected(ComponentName name, IBinder service) {
            mService = (GlassSensorRecordService.SensorRecordBinder) service;
        }

        @Override
        public void onServiceDisconnected(ComponentName name) {
        }
    };

    private void startRecord(List<Integer> sensorNeedRecord) {
        Intent intent = new Intent(this, GlassSensorRecordService.class);
        bindService(intent, mConnection, BIND_AUTO_CREATE);
    }

    private void stopRecord() {
        if (mService != null) {
            mService.stop();
            unbindService(mConnection);
        }
    }

    @Override
    public void onCreate() {
        super.onCreate();
        //1.获取SensorManager实例
        mSensorManager = (SensorManager) this.getSystemService(Service.SENSOR_SERVICE);
        mSensorEventListener = new MySensorEventListener();
        mSensorDataWriter = new SensorDataWriter("mobile");
        startRecord(null);
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
            float secondToBegin = (System.currentTimeMillis() - startTime) / 1000.00f; //计算从任务开始到现在的用时
            SensorRecord sensorRecord = new SensorRecord(event, System.currentTimeMillis(), secondToBegin);
            SensorDataWriter.mSensorRecordQueue.offer(sensorRecord);

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
         * @param  {@link Sensor}中代表传感器的常量如{@link Sensor#TYPE_ACCELEROMETER}
         * @return 无法监听的传感器对应的常量
         */
        public List<Integer> start() {
            //2.注册对应传感器的监听器
            List<Integer> cannotRegister = new ArrayList<>();
            Log.d(TAG, "start: " + typeList.isEmpty());
            for (int sensorType : typeList)
                mSensorManager.registerListener(mSensorEventListener,
                        mSensorManager.getDefaultSensor(sensorType),
                        SensorManager.SENSOR_DELAY_FASTEST);

            startTime = System.currentTimeMillis();
            return cannotRegister.isEmpty() ? null : cannotRegister;
        }

        public void stop() {
            startTime = 0;
            mSensorManager.unregisterListener(mSensorEventListener);
            try {
                mSensorDataWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
