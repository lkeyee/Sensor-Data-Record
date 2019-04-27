package com.ssmc.sensorrecord;

import android.hardware.Sensor;
import android.os.Environment;
import android.print.PrinterId;
import android.util.Log;

import com.ssmc.sensordesc.SensorRecord;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static android.content.ContentValues.TAG;

/**
 * 记录传感器数据，并统合到一起
 */
public class SensorDataWriter implements ISensorStorage {

    //private final String TAG = getClass().getSimpleName();

    private boolean isConnected;

    private static String PATH = Environment.getExternalStorageDirectory().getAbsolutePath();
    private static final String FILE_NAME_SAME_TIME = "SameTime";
    private static final String KEY_SAME_TIME = "SameTimeXXX";

    private static final int MIN_TIME_INTERVAL = 10;

    private static int count=0;
    private final int PORT=7220;
    private final String HOST="172.31.73.80"; //pc的ip地址

    private String mFilePrefix;//文件名前缀

    private Executor mExecutor = Executors.newSingleThreadExecutor();
    private BlockingQueue<SensorRecord> mSensorRecordQueue = new LinkedBlockingQueue<>();
    private Map<String, BufferedWriter> mSensorTypeMapToOutput = new HashMap<>();
    private Map<String, BufferedWriter> mSensorTypeMapToServer=new HashMap<>();
    private boolean isRunning = false;

    class StorageTask implements Runnable {
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS", Locale.getDefault());
        DecimalFormat decimalFormat = new DecimalFormat("0.000");
        private long previousTimeStamp = 0;

        @Override
        public void run() {

            while (isRunning) {
                try {
                    SensorRecord sensorRecord = mSensorRecordQueue.take();
                    //数据写入到单个文件
                    BufferedWriter out = mSensorTypeMapToOutput.get(sensorRecord.getStringType());
                    // 将数据传送给服务器
                    BufferedWriter bw=mSensorTypeMapToServer.get(sensorRecord.getStringType());
                    writeToSingleFile(out, sensorRecord);
                    transferSingleFileToServer(bw,sensorRecord);
                    //时间相同的记录整合在一起
                    long timeStamp = sensorRecord.getTimeStamp();
                    Log.i(TAG, "run: timeStamp="+timeStamp);
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
            writerSameTime.write( dateFormatter.format(new Date(sensorRecord.getTimeStamp()))+ " ");
            writerSameTime.write(type.substring(type.lastIndexOf('.')) + " : ");

            for (float value : sensorRecord.getValues()) {
                writerSameTime.write(decimalFormat.format(value) + " ");
            }
            writerSameTime.write("\n");
            writerSameTime.flush();
        }

        private synchronized void writeSameTimeRecordHead(BufferedWriter writerSameTime, long timeStamp) throws IOException {
            String time = dateFormatter.format(new Date(timeStamp));
            writerSameTime.write("START\n");
            writerSameTime.write(time + "\n");
            writerSameTime.flush();
        }

        private synchronized void writeSameTimeRecordTail(BufferedWriter writerSameTime) throws IOException {
            writerSameTime.write("END\n");
            writerSameTime.flush();
        }

        /**
         * 将文件写入对应的本地文件
         */
        private synchronized void writeToSingleFile(BufferedWriter writer, SensorRecord sensorRecord) throws IOException {
            String timeToBegin = decimalFormat.format(sensorRecord.getTimeStamp());
            String time = dateFormatter.format(new Date(sensorRecord.getTimeStamp()));
            writer.write(timeToBegin + "  ");
            writer.write(time + "  ");
            for (float value : sensorRecord.getValues()) {
                writer.write(decimalFormat.format(value) + " ");
            }
            writer.write("\n");
            writer.flush();
        }

        private synchronized void transferSingleFileToServer(BufferedWriter bw,SensorRecord sensorRecord) throws IOException{
            String timeToBegin = decimalFormat.format(sensorRecord.getTimeToBeginSecond());
            String time = dateFormatter.format(new Date(sensorRecord.getTimeStamp()));
            StringBuffer sb=new StringBuffer();
            sb.append(timeToBegin+" "+time+" ");

            for (float value : sensorRecord.getValues()) {
                sb.append(decimalFormat.format(value) + " ");
            }
            sb.append("\n");
            bw.write(sb.toString());
            bw.flush();
        }
    }

    private void createSameTime() {
        try {
            BufferedWriter out = createOutputStream(PATH+"/"+mFilePrefix+"_SensorData",  FILE_NAME_SAME_TIME);
            mSensorTypeMapToOutput.put(KEY_SAME_TIME, out);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public SensorDataWriter() {
        createSameTime();
    }

    public SensorDataWriter(String mFilePrefix1) {
        this.mFilePrefix=mFilePrefix1;
        createSameTime();
        count++;
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
        Log.i(TAG, "writeSensorData:： 类型="+sensorRecord.getStringType());
        if (!mSensorTypeMapToOutput.containsKey(sensorType) ) {
            //用hashTable来保存输出流与传感器类型的映射关系
            BufferedWriter out = createOutputStream(PATH+"/"+mFilePrefix+"_SensorData", sensorRecord.getStringType().substring(15));
            Socket socket=new Socket(HOST,PORT);
            BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            bw.write(mFilePrefix+"_"+sensorRecord.getStringType().substring(15)+count+"\n");
            bw.flush();
            mSensorTypeMapToOutput.put(sensorRecord.getStringType(), out);
            mSensorTypeMapToServer.put(sensorRecord.getStringType(),bw);
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
        return new BufferedWriter(new FileWriter(fileWrite));
    }

    public void setConnected(boolean isConnected) {
        this.isConnected = isConnected;
    }
}
