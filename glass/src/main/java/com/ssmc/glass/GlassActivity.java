package com.ssmc.glass;

import android.app.Service;
import android.bluetooth.BluetoothAdapter;
import android.content.ComponentName;
import android.content.Intent;
import android.content.ServiceConnection;
import android.hardware.Sensor;
import android.hardware.SensorEvent;
import android.hardware.SensorEventListener;
import android.hardware.SensorManager;
import android.os.Environment;
import android.os.IBinder;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.Toast;

import com.ssmc.sensordesc.SensorRecord;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.List;

public class GlassActivity extends AppCompatActivity {

    private static final int REQUEST_OPEN_BT_CODE = 1;

    private static final String PATH = Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_DOWNLOADS).getAbsolutePath();
    private long startTime;

    File file = new File(PATH + File.separator + "test.txt");
    BluetoothService mBluetoothService;

    BluetoothAdapter mBluetoothAdapter = BluetoothAdapter.getDefaultAdapter();

    private List<Integer> sensorNeedRecord;
    private GlassSensorRecordService.SensorRecordBinder mService;
    //通过ServiceConnection来监听与service的连接
    private ServiceConnection mConnection = new ServiceConnection() {
        @Override
        public void onServiceConnected(ComponentName name, IBinder service) {
            mService = (GlassSensorRecordService.SensorRecordBinder) service;
            mService.start();
        }

        @Override
        public void onServiceDisconnected(ComponentName name) {
        }
    };

    @Override
    public void onResume() {
        super.onResume();
        // Performing this check in onResume() covers the case in which BT was
        // not enabled during onStart(), so we were paused to enable it...
        // onResume() will be called when ACTION_REQUEST_ENABLE activity returns.
        if (mBluetoothService != null) {
            // Only if the state is STATE_NONE, do we know that we haven't started already
            if (mBluetoothService.getState() == BluetoothService.STATE_NONE) {
                // Start the Bluetooth chat services
                mBluetoothService.start();
            }
        }
    }

    private void startRecord() {
        Intent intent = new Intent(GlassActivity.this, GlassSensorRecordService.class);
        bindService(intent, mConnection, BIND_AUTO_CREATE);
    }

    private void stopRecord() {
        if (mService != null) {
            mService.stop();
            unbindService(mConnection);
        }
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        //蓝牙是否开启
        if (mBluetoothAdapter == null) {
            Toast.makeText(GlassActivity.this, "设备不支持蓝牙", Toast.LENGTH_SHORT).show();
        } else {
            //进行下一步操作
            if (!mBluetoothAdapter.isEnabled()) {
                //请求开启设备蓝牙
                Intent openBT = new Intent(BluetoothAdapter.ACTION_REQUEST_ENABLE);
                startActivityForResult(openBT, REQUEST_OPEN_BT_CODE);
            }
        }
        mBluetoothService = new BluetoothService();
        mBluetoothService.start();

        //重启连接蓝牙的线程
        Button reconnect = findViewById(R.id.reconnect);
        reconnect.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                mBluetoothService = new BluetoothService();
                mBluetoothService.start();
            }
        });

        Button bt_connect = findViewById(R.id.connect);
        bt_connect.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                startRecord();
            }
        });
        startRecord();

//        SensorManager mSensorManager = (SensorManager) getSystemService(Service.SENSOR_SERVICE);
//
//        assert mSensorManager != null;
//        mSensorManager.registerListener(new SensorEventListener() {
//                                            @Override
//                                            public void onSensorChanged(SensorEvent event) {
//                                                startTime = System.currentTimeMillis();
//                                                float secondToBegin = (System.currentTimeMillis() - startTime) / 1000.00f; //计算从任务开始到现在的用时
//                                                SensorRecord sensorRecord = new SensorRecord(event, System.currentTimeMillis(), secondToBegin);
//                                                mBluetoothService.write(sensorRecord);
//                                            }
//
//                                            @Override
//                                            public void onAccuracyChanged(Sensor sensor, int accuracy) {
//
//                                            }
//                                        },
//                mSensorManager.getDefaultSensor(Sensor.TYPE_GRAVITY),
//                SensorManager.SENSOR_DELAY_NORMAL);
    }


}


