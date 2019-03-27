package com.ssmc.sensorrecord.mobileUI;

import android.Manifest;
import android.annotation.SuppressLint;
import android.app.ActionBar;
import android.app.Activity;
import android.bluetooth.BluetoothAdapter;
import android.bluetooth.BluetoothDevice;
import android.bluetooth.BluetoothGatt;
import android.bluetooth.BluetoothGattCallback;
import android.bluetooth.BluetoothGattCharacteristic;
import android.bluetooth.BluetoothGattService;
import android.bluetooth.BluetoothManager;
import android.bluetooth.BluetoothProfile;
import android.content.ComponentName;
import android.content.Context;
import android.content.DialogInterface;
import android.content.Intent;
import android.content.ServiceConnection;
import android.hardware.Sensor;
import android.os.Handler;
import android.os.IBinder;
import android.os.Message;
import android.support.v4.app.FragmentActivity;
import android.support.v7.app.AlertDialog;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.util.Log;
import android.view.View;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.LinearLayout;
import android.widget.Toast;

import com.ssmc.sensordesc.SensorRecord;
import com.ssmc.sensorrecord.MobileSensorRecordService;
import com.ssmc.sensorrecord.R;
import com.ssmc.sensorrecord.SensorDataWriter;
import com.ssmc.sensorrecord.WearTransfer;
import com.ssmc.sensorrecord.bluetooth.BluetoothService;
import com.ssmc.sensorrecord.bluetooth.Constants;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import permissions.dispatcher.NeedsPermission;
import permissions.dispatcher.RuntimePermissions;


/**
 * 这个Activity逻辑过于复杂，需要分模块解耦合
 * 建议分成本地Sensor、Wear
 */
@RuntimePermissions
public class MobileActivity extends AppCompatActivity {

    /**
     * Name of the connected device
     */
    private String mConnectedDeviceName = null;

    /**
     * Array adapter for the conversation thread
     */

    /**
     * String buffer for outgoing messages
     */
    private StringBuffer mOutStringBuffer;

    /**
     * Local Bluetooth adapter
     */
    private BluetoothAdapter mBluetoothAdapter = null;

    /**
     * Member object for the chat services
     */

    private static final int REQUEST_CONNECT_DEVICE_SECURE = 1;
    private static final int REQUEST_CONNECT_DEVICE_INSECURE = 2;
    private static final int REQUEST_ENABLE_BT = 3;

    private static final String TAG = "MobileActivity";
    BluetoothService btService;
    BluetoothService btService1;

    private Socket socket1,socket2;
//    private static final int PORT=7200;   //端口号
//    private static final String HOST="172.31.73.90";  //服务器地址


    //在这里设置想提供给用户的监听接口
    //保存 CheckBox 和 Sensor 的映射
    private List<SensorCheckBox> mCheckboxToSensorMapping = new ArrayList<>(Arrays.asList(
            new SensorCheckBox(Sensor.STRING_TYPE_ACCELEROMETER, Sensor.TYPE_ACCELEROMETER, this),
            new SensorCheckBox(Sensor.STRING_TYPE_GRAVITY, Sensor.TYPE_GRAVITY, this),
            new SensorCheckBox(Sensor.STRING_TYPE_GYROSCOPE, Sensor.TYPE_GYROSCOPE, this),
            new SensorCheckBox(Sensor.STRING_TYPE_LINEAR_ACCELERATION, Sensor.TYPE_LINEAR_ACCELERATION, this),
            new SensorCheckBox(Sensor.STRING_TYPE_ROTATION_VECTOR, Sensor.TYPE_ROTATION_VECTOR, this),
            new SensorCheckBox(Sensor.STRING_TYPE_MAGNETIC_FIELD, Sensor.TYPE_MAGNETIC_FIELD, this),
            new SensorCheckBox(Sensor.STRING_TYPE_ORIENTATION, Sensor.TYPE_ORIENTATION, this),
            new SensorCheckBox(Sensor.STRING_TYPE_PRESSURE,Sensor.TYPE_PRESSURE,this)));

    private Button record;
    private Button connect_bt;
    private Button submit_bt;
    private WearTransfer mWearTransfer1,mWearTransfer2;
    private String deviceName1=null,deviceName2=null;
    private MobileSensorRecordService.SensorRecordBinder mService;
    private List<Integer> sensorNeedRecord;

    //通过ServiceConnection来监听与service的连接
    private ServiceConnection mConnection = new ServiceConnection() {
        @Override
        public void onServiceConnected(ComponentName name, IBinder service) {
            mService = (MobileSensorRecordService.SensorRecordBinder) service;
            mService.start(sensorNeedRecord);
        }

        @Override
        public void onServiceDisconnected(ComponentName name) {
        }
    };


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        mBluetoothAdapter = BluetoothAdapter.getDefaultAdapter();
        btService = new BluetoothService(this, mHandler,0);
        btService1=new BluetoothService(this,mHandler1,1);
        intiViews();



    }

    @Override
    protected void onResume() {
        super.onResume();

    }

    @Override
    protected void onPause() {
        super.onPause();
    }

    @Override
    protected void onStop() {
        super.onStop();
        stopRecord();
    }

    /**
     * 动态加载布局
     */
    @NeedsPermission({Manifest.permission.WRITE_EXTERNAL_STORAGE,Manifest.permission.READ_EXTERNAL_STORAGE})
    void intiViews() {
        LinearLayout layout = new LinearLayout(this);
        layout.setOrientation(LinearLayout.VERTICAL);
        for (SensorCheckBox sensorCheckBox : mCheckboxToSensorMapping) {
            sensorCheckBox.loadCheckBox();//加载文字到checkbox上
            layout.addView(sensorCheckBox.checkBox);
        }
        record = new Button(this);
        record.setText("确定");
        record.setWidth(LinearLayout.LayoutParams.MATCH_PARENT);
        record.setHeight(LinearLayout.LayoutParams.WRAP_CONTENT);
        record.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                MobileActivityPermissionsDispatcher.recordSensorWithPermissionCheck(MobileActivity.this);
            }
        });

        connect_bt = new Button(this);
        connect_bt.setText("蓝牙连接");
        connect_bt.setWidth(LinearLayout.LayoutParams.MATCH_PARENT);
        connect_bt.setHeight(LinearLayout.LayoutParams.WRAP_CONTENT);
        connect_bt.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                btService.isBTOpen();
                btService.onClickBuletoothButton();

            }
        });

        layout.addView(record);
        layout.addView(connect_bt);
        setContentView(layout);
    }

    /**
     * 传感器记录
     */
    @NeedsPermission({Manifest.permission.WRITE_EXTERNAL_STORAGE,
            Manifest.permission.READ_EXTERNAL_STORAGE,
            Manifest.permission.BODY_SENSORS})
    void recordSensor() {
        if (record.getText().equals("确定")) {
            StringBuilder stringBuilder = new StringBuilder();
            final List<Integer> sensorNeedRecord = new LinkedList<>();
            stringBuilder.append("已经选中:\n");
            for (SensorCheckBox sensorCheckBox : mCheckboxToSensorMapping) {
                if (sensorCheckBox.checkBox.isChecked()) {
                    stringBuilder.append(sensorCheckBox.checkBox.getText().toString());
                    stringBuilder.append("\n");
                    sensorNeedRecord.add(sensorCheckBox.sensor);
                }
            }
            new AlertDialog.Builder(MobileActivity.this)
                    .setMessage(stringBuilder.toString())
                    .setPositiveButton("确定", new DialogInterface.OnClickListener() {
                        @Override
                        public void onClick(DialogInterface dialog, int which) {
                            startRecord(sensorNeedRecord);
                        }
                    })
                    .create()
                    .show();
            record.setText("正在写入");
        } else if (record.getText().equals("正在写入")) {
            stopRecord();
            record.setText("确定");
        }
    }

    private void stopRecord() {
        if (mService != null) {
            mService.stop();
            unbindService(mConnection);
        }
    }

    private void startRecord(List<Integer> sensorNeedRecord) {
        this.sensorNeedRecord = sensorNeedRecord;
        Intent intent = new Intent(MobileActivity.this, MobileSensorRecordService.class);
        bindService(intent, mConnection, BIND_AUTO_CREATE);
    }

    /**
     * Establish connection with other device
     *
     * @param data   An {@link Intent} with {@link DeviceListActivity#EXTRA_DEVICE_ADDRESS} extra.
     * @param secure Socket Security type - Secure (true) , Insecure (false)
     */


    private void connectDevice(Intent data, boolean secure) {

//         Get the device MAC address

        Bundle bundle=data.getExtras();

        String [] addressArray=bundle.getStringArray(DeviceListActivity.DEVICE_ADDRESS_BUNDLE);
        String [] nameArray=bundle.getStringArray(DeviceListActivity.DEVICE_NAME_BUNDLE);
        BluetoothDevice [] devices=new BluetoothDevice[addressArray.length];
        for(int i=0;i<addressArray.length;i++) {
            devices[i] = mBluetoothAdapter.getRemoteDevice(addressArray[i]);
            Log.i(TAG, "connectDevice: i="+i+" "+addressArray[i]);
            if(i==0) {
                btService.connect(devices[i],secure,0);
                deviceName1=nameArray[i];
                MobileSensorRecordService.mSensorDataWriter1 = new SensorDataWriter(deviceName1);
            }
            else {
                btService1.connect(devices[i], secure,1);
                deviceName2=nameArray[i];
                MobileSensorRecordService.mSensorDataWriter2 = new SensorDataWriter(deviceName2);

            }
        }

    }



    public void onActivityResult(int requestCode, int resultCode, Intent data) {
        switch (requestCode) {
            case REQUEST_CONNECT_DEVICE_SECURE:
                // When DeviceListActivity returns with a device to connect
                if (resultCode == Activity.RESULT_OK) {
                    connectDevice(data, true);
                }
                break;
            case REQUEST_CONNECT_DEVICE_INSECURE:
                // When DeviceListActivity returns with a device to connect
                if (resultCode == Activity.RESULT_OK) {
                    connectDevice(data, false);
                }
                break;
            case REQUEST_ENABLE_BT:
                // When the request to enable Bluetooth returns
                if (resultCode == Activity.RESULT_OK) {
                    // Bluetooth is now enabled, so set up a chat session
                } else {
                    // User did not enable Bluetooth or an error occurred
                    Log.d(TAG, "BT not enabled");
                    Toast.makeText(this, "Bluetooth was not enabled. Leaving Bluetooth Chat.",
                            Toast.LENGTH_SHORT).show();
                    this.finish();
                }
        }
    }

    @SuppressLint("HandlerLeak")
    private final Handler mHandler = new Handler() {
        @Override
        public void handleMessage(Message msg) {
            switch (msg.what) {
                case Constants.MESSAGE_DEVICE_NAME:
                    // save the connected device's name
                    mConnectedDeviceName = msg.getData().getString(Constants.DEVICE_NAME);
                    Toast.makeText(MobileActivity.this, "Connected to "
                            + mConnectedDeviceName, Toast.LENGTH_SHORT).show();
                    break;
                case Constants.MESSAGE_TOAST:
                    Toast.makeText(MobileActivity.this, msg.getData().getString(Constants.TOAST),
                            Toast.LENGTH_SHORT).show();
                    break;
            }
        }
    };

    @SuppressLint("HandlerLeak")
    private final Handler mHandler1 = new Handler() {
        @Override
        public void handleMessage(Message msg) {
            switch (msg.what) {
                case Constants.MESSAGE_DEVICE_NAME:
                    // save the connected device's name
                    mConnectedDeviceName = msg.getData().getString(Constants.DEVICE_NAME);
                    Toast.makeText(MobileActivity.this, "Connected to "
                            + mConnectedDeviceName, Toast.LENGTH_SHORT).show();
                    break;
                case Constants.MESSAGE_TOAST:
                    Toast.makeText(MobileActivity.this, msg.getData().getString(Constants.TOAST),
                            Toast.LENGTH_SHORT).show();
                    break;
            }
        }
    };
}
