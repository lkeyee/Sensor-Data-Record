package com.ssmc.glass;

import android.os.Environment;

import com.ssmc.sensordesc.SensorRecord;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 记录传感器数据，并统合到一起
 */
public class SensorDataWriter implements ISensorStorage {

    //private final String TAG = getClass().getSimpleName();

    private boolean isConnected;

    private static final String PATH = Environment.getExternalStorageDirectory().getAbsolutePath();
    private static final String FILE_NAME_SAME_TIME = "SameTime";
    private static final String KEY_SAME_TIME = "SameTimeXXX";
    private static final int MIN_TIME_INTERVAL = 3;

    private String mFilePrefix;//文件名前缀

    private Executor mExecutor = Executors.newSingleThreadExecutor();
    public static BlockingQueue<SensorRecord> mSensorRecordQueue = new LinkedBlockingQueue<>();
    private Map<String, BufferedWriter> mSensorTypeMapToOutput = new HashMap<>();
    private boolean isRunning = false;

    class StorageTask implements Runnable {
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss", Locale.getDefault());
        DecimalFormat decimalFormat = new DecimalFormat("0.000");
        private long previousTimeStamp = 0;

        @Override
        public void run() {
            while (isRunning) {
                try {
                    SensorRecord sensorRecord = mSensorRecordQueue.take();
                    //数据写入到单个文件
                    BufferedWriter out = mSensorTypeMapToOutput.get(sensorRecord.getStringType());
                    writeToSingleFile(out, sensorRecord);
                    //时间相同的记录整合在一起
                    long timeStamp = sensorRecord.getTimeStamp();
                    BufferedWriter sameTimeWriter = mSensorTypeMapToOutput.get(KEY_SAME_TIME);
                    if (timeStamp - previousTimeStamp > MIN_TIME_INTERVAL) {
                        //两个记录的时间有差
                        previousTimeStamp = timeStamp;
                        writeSameTimeRecordTail(sameTimeWriter);
                        writeSameTimeRecordHead(sameTimeWriter, timeStamp);
                    } else {
                        //两个记录的时间相同
                        writeToSameTimeFile(sameTimeWriter, sensorRecord);
                    }
                } catch (InterruptedException | IOException e) {
                    e.printStackTrace();
                }
            }
        }

        /**
         * 将不同类型同一时间的传感器组合到一起
         */
        private synchronized void writeToSameTimeFile(BufferedWriter writerSameTime, SensorRecord sensorRecord) throws IOException {
            String type = sensorRecord.getStringType();
            writerSameTime.write(type + " : ");
            for (float value : sensorRecord.getValues()) {
                writerSameTime.write(decimalFormat.format(value) + " ");
            }
            writerSameTime.write("\n");
        }

        private synchronized void writeSameTimeRecordHead(BufferedWriter writerSameTime, long timeStamp) throws IOException {
            String time = dateFormatter.format(new Date(timeStamp));
            writerSameTime.write("START\n");
            writerSameTime.write(time + "\n");
        }

        private synchronized void writeSameTimeRecordTail(BufferedWriter writerSameTime) throws IOException {
            writerSameTime.write("END\n");
        }

        /**
         * 将文件写入对应的本地文件
         */
        private synchronized void writeToSingleFile(BufferedWriter writer, SensorRecord sensorRecord) throws IOException {
            String timeToBegin = decimalFormat.format(sensorRecord.getTimeToBeginSecond());
            String time = dateFormatter.format(new Date(sensorRecord.getTimeStamp()));
            writer.write(timeToBegin + "  ");
            writer.write(time + "  ");
            for (float value : sensorRecord.getValues()) {
                writer.write(decimalFormat.format(value) + " ");
            }
            writer.write("\n");
        }
    }


    public SensorDataWriter(String mFilePrefix) {
        this.mFilePrefix = mFilePrefix;
        try {
            BufferedWriter out = createOutputStream(PATH, mFilePrefix + "_" + FILE_NAME_SAME_TIME);
            mSensorTypeMapToOutput.put(KEY_SAME_TIME, out);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public SensorDataWriter(String mFilePrefix, BufferedWriter bt_out) {
        this.mFilePrefix = mFilePrefix;
        BufferedWriter out = bt_out;
        mSensorTypeMapToOutput.put(KEY_SAME_TIME, out);
    }

    /**
     * 添加到待写入队列中，写入本地文件
     */
    @Override
    public void writeSensorData(SensorRecord sensorRecord) throws IOException {
        if (!isRunning) {
            isRunning = true;
            mExecutor.execute(new StorageTask());
        }
        String sensorType = sensorRecord.getStringType();


        if (!mSensorTypeMapToOutput.containsKey(sensorType)) {
            //用hashTable来保存输出流与传感器类型的映射关系
            BufferedWriter out = createOutputStream(PATH, mFilePrefix + "_" + sensorRecord.getStringType());
            mSensorTypeMapToOutput.put(sensorRecord.getStringType(), out);
        }
        mSensorRecordQueue.offer(sensorRecord);
    }

    @Override
    public void close() throws IOException {
        isRunning = false;
        Collection<BufferedWriter> outputStreams = mSensorTypeMapToOutput.values();
        for (BufferedWriter out : outputStreams) {
            out.flush();
            out.close();
        }
    }

    /**
     * 构造输出流
     */
    private BufferedWriter createOutputStream(String path, String name) throws IOException {
        File fileWrite = new File(path + File.separator + name + ".txt");
        fileWrite.getParentFile().mkdirs();
        if (fileWrite.exists())
            fileWrite.delete();
        fileWrite.createNewFile();
        return new BufferedWriter(new FileWriter(fileWrite));
    }

    public void setConnected(boolean isConnected) {
        this.isConnected = isConnected;
    }
}
