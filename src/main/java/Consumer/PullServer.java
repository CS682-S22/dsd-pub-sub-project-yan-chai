package Consumer;

import Model.Connection;
import Model.DataRecord;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingDeque;

public class PullServer implements Runnable{

    private String broker;
    private int port;
    private String topic;
    LinkedBlockingDeque<byte[]> bQueue;
    private int start;

    public PullServer(String b, int p, String t, LinkedBlockingDeque<byte[]> queue, int s) {
        Properties prop = new Properties();
        try {
            prop.load(new FileReader("config.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        broker = b;
        port = p;
        topic = t;
        bQueue = queue;
        start = s;
    }


    @Override
    public void run() {
        Connection connection;
        while (true) {
            connection = new Connection(broker, port);
            connection.send(DataRecord.Record.newBuilder().setTopic("consumer").setMsg(ByteString.EMPTY).build().toByteArray());
            connection.send(DataRecord.Record.newBuilder().setTopic(topic).setMsg(ByteString.copyFrom(String.valueOf(start).getBytes(StandardCharsets.UTF_8))).build().toByteArray());
            byte[] tmp = connection.receive();
            DataRecord.Record record = null;
            try {
                record = DataRecord.Record.parseFrom(tmp);
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
            start += record.getMsg().toByteArray().length;
            bQueue.add(record.getMsg().toByteArray());
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
