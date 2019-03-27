package com.ssmc.sensorrecord;

import android.content.ComponentName;
import android.content.Intent;
import android.content.ServiceConnection;
import android.hardware.Sensor;
import android.os.Bundle;
import android.os.IBinder;
import android.support.v7.app.AppCompatActivity;
import android.support.wearable.activity.WearableActivity;
import android.view.View;
import android.widget.Button;
import android.widget.Toast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class WearActivity extends WearableActivity {

    private Button btRecordSensor;

    private boolean isRunning = false;

    private List<Integer> mListSensorNeedRecord = new ArrayList<>(Arrays.asList(
            Sensor.TYPE_ACCELEROMETER,
            Sensor.TYPE_GRAVITY,
            Sensor.TYPE_GYROSCOPE,
            Sensor.TYPE_LINEAR_ACCELERATION,
            Sensor.TYPE_ROTATION_VECTOR,
            Sensor.TYPE_MAGNETIC_FIELD,
            Sensor.TYPE_ORIENTATION
    ));
    private WearSensorRecordService.SensorRecordBinder mSensorRecordBinder;

    private ServiceConnection mSensorRecordServiceConnection = new ServiceConnection() {
        @Override
        public void onServiceConnected(ComponentName name, IBinder service) {
            mSensorRecordBinder = (WearSensorRecordService.SensorRecordBinder) service;
            Set<Integer> sensorTypesNeedRecord = new HashSet<>(mListSensorNeedRecord);
            mSensorRecordBinder.start(sensorTypesNeedRecord);
        }

        @Override
        public void onServiceDisconnected(ComponentName name) {

        }
    };

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_wear);
        btRecordSensor = findViewById(R.id.bt_record_sensor);
        btRecordSensor.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                if (!isRunning) {
                    startRecord();
                }
            }
        });
    }


    private void startRecord() {
        Intent intent = new Intent(WearActivity.this, WearSensorRecordService.class);
        bindService(intent, mSensorRecordServiceConnection, BIND_AUTO_CREATE);
        btRecordSensor.setText(R.string.sensor_record_status_running);
        isRunning = true;
    }

    private void stopRecord() {
        isRunning = false;
        mSensorRecordBinder.stop();
        unbindService(mSensorRecordServiceConnection);
        btRecordSensor.setText(R.string.sensor_record_status_not_running);
        Toast.makeText(WearActivity.this, R.string.sensor_record_tip_stop, Toast.LENGTH_SHORT).show();
    }

    @Override
    protected void onStop() {
        super.onStop();
        if (isRunning) {
            stopRecord();
        }
    }
}

