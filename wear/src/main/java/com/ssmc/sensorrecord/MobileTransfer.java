package com.ssmc.sensorrecord;

import android.content.Context;
import android.os.Bundle;
import android.util.Log;

import com.google.android.gms.common.ConnectionResult;
import com.google.android.gms.common.api.GoogleApiClient;
import com.google.android.gms.wearable.DataMap;
import com.google.android.gms.wearable.PutDataMapRequest;
import com.google.android.gms.wearable.PutDataRequest;
import com.google.android.gms.wearable.Wearable;
import com.ssmc.sensordesc.SensorRecord;

/**
 * 与手机的数据通信处理
 */

class MobileTransfer {

    private static final String TAG = "MobileTransfer";
    private GoogleApiClient mGoogleApiClient;

    MobileTransfer(Context context) {
        mGoogleApiClient = new GoogleApiClient.Builder(context)
                .addConnectionCallbacks(new GoogleApiClient.ConnectionCallbacks() {
                    @Override
                    public void onConnected(Bundle connectionHint) {
                        Log.d(TAG, "onConnected: " + connectionHint);
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

    void connect(){
        mGoogleApiClient.connect();
    }

    void disConnect(){
        mGoogleApiClient.disconnect();
    }

    void sendSensorData(SensorRecord sensorData) {
        PutDataMapRequest putDataMapReq = PutDataMapRequest.create("/sensor_data");
        DataMap dataMap = putDataMapReq.getDataMap();
        //下面待改进
        dataMap.putString("stringType", sensorData.getStringType());
        dataMap.putFloatArray("values", sensorData.getValues());
        dataMap.putFloat("timeToBeginSecond", sensorData.getTimeToBeginSecond());
        dataMap.putLong("timeStamp", sensorData.getTimeStamp());
        dataMap.putInt("type", sensorData.getType());
        PutDataRequest request = putDataMapReq.asPutDataRequest();
        Wearable.DataApi.putDataItem(mGoogleApiClient, request);
    }
}
