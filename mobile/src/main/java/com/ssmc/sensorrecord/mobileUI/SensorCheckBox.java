package com.ssmc.sensorrecord.mobileUI;

import android.content.Context;
import android.hardware.Sensor;
import android.widget.CheckBox;

/**
 * 建立 Checkbox 与 Sensor 的映射关系
 */
public class SensorCheckBox {
    CheckBox checkBox;
    int sensor;
    private Context context;
    private String checkBoxText;

    /**
     * 构造方法
     *
     * @param checkBoxText 需要 CheckBox 展示的文字
     * @param sensor       {@link Sensor#getType()}
     */
    SensorCheckBox(String checkBoxText, int sensor, Context context) {
        this.checkBoxText = checkBoxText;
        this.context = context;
        this.sensor = sensor;
    }

    /**
     * 加载 CheckBox 并初始化它
     */
    void loadCheckBox() {
        checkBox = new CheckBox(context);
        checkBox.setText(checkBoxText);
    }
}
