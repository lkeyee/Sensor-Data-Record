package com.ssmc.sensorrecord;

import android.content.Context;
import android.os.Bundle;
import android.util.Log;

import com.google.android.gms.common.ConnectionResult;
import com.google.android.gms.common.api.GoogleApiClient;
import com.google.android.gms.wearable.DataApi;
import com.google.android.gms.wearable.DataEvent;
import com.google.android.gms.wearable.DataEventBuffer;
import com.google.android.gms.wearable.DataItem;
import com.google.android.gms.wearable.DataMap;
import com.google.android.gms.wearable.DataMapItem;
import com.google.android.gms.wearable.Wearable;
import com.ssmc.sensordesc.SensorRecord;

import java.io.IOException;

/**
 * 与手表的数据通信处理
 */

public class WearTransfer implements DataApi.DataListener {

    private static final String TAG = "WearTransfer";
    private ISensorStorage mSensorDataWriter;
    private GoogleApiClient mGoogleApiClient;

    public WearTransfer(Context context, ISensorStorage sensorStorage) {
        mSensorDataWriter = sensorStorage;
        mGoogleApiClient = new GoogleApiClient.Builder(context)
                .addConnectionCallbacks(new GoogleApiClient.ConnectionCallbacks() {
                    @Override
                    public void onConnected(Bundle connectionHint) {
                        Log.d(TAG, "onConnected: " + connectionHint);
                        Wearable.DataApi.addListener(mGoogleApiClient, WearTransfer.this);
                        // Now you can use the Data Layer API
                    }

                    @Override
                    public void onConnectionSuspended(int cause) {
                        Log.d(TAG, "onConnectionSuspended: " + cause);
                    }
                })
                .addOnConnectionFailedListener(new GoogleApiClient.OnConnectionFailedListener() {
                    @Override
                    public void onConnectionFailed(ConnectionResult result) {
                        Log.d(TAG, "onConnectionFailed: " + result);
                    }
                })
                // Request access only to the Wearable API
                .addApi(Wearable.API)
                .build();
    }

    public void start() {
        mGoogleApiClient.connect();
    }

    public void stop() {
        Wearable.DataApi.removeListener(mGoogleApiClient, this);
        mGoogleApiClient.disconnect();
        if (mSensorDataWriter != null) {
            try {
                mSensorDataWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void saveSensorData(SensorRecord sensorRecord) {
        try {
            mSensorDataWriter.writeSensorData(sensorRecord);
            Log.i(TAG, "saveSensorData: 来自设备的数据写入");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onDataChanged(DataEventBuffer dataEventBuffer) {
        for (DataEvent event : dataEventBuffer) {
            Log.i(TAG, "onDataChanged: 状态为："+event.getType());
            if (event.getType() == DataEvent.TYPE_CHANGED) {
                // DataItem changed
                DataItem item = event.getDataItem();
                DataMap dataMap = DataMapItem.fromDataItem(item).getDataMap();
                ///有待改进
                String stringType = dataMap.get("stringType");
                float[] values = dataMap.get("values");
                float timeToBeginSecond = dataMap.get("timeToBeginSecond");
                long timeStamp = dataMap.get("timeStamp");
                int type = dataMap.get("type");
                SensorRecord sensorRecord = new SensorRecord(stringType, values,
                        timeToBeginSecond, timeStamp, type);
                Log.d(TAG, "onDataChanged: " + sensorRecord.toString());
                saveSensorData(sensorRecord);
            }
        }
    }
}
