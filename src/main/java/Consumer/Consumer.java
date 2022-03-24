package Consumer;

import Broker.Broker;
import Model.Connection;
import Model.DataRecord;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Author: Haoyu Yan
 * pull base consumer
 */
public class Consumer {

    private String broker;
    private int port;
    private String topic;
    private int start;
    private LinkedBlockingDeque<byte[]> bQueue;
    private int time;
    private String[] records;
    private int index;
    private static final Logger logger = LogManager.getLogger(Broker.class);


    public Consumer(String b, int p, String t, int startPosition) {
        broker = b;
        port = p;
        topic = t;
        start = startPosition;
        bQueue = new LinkedBlockingDeque<>();
        records = new String[1];
        records[0] = "";
        index = 0;
    }

    public String poll(Duration duration) {
        byte[] tmp = null;
        if (records.length <= (index + 1)) {
            pull();
            try {
                tmp = bQueue.poll(duration.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (tmp == null) {
                System.out.println("Nothing in Consumer");
            } else {
                if (index >= records.length) {
                    records = new String[1];
                    records[0] = "";
                    index = 0;
                    System.out.println("Nothing");
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
                String last = records[index];
                records = new String(tmp).split("\n");
                records[0] = last + records[0];
            }
            index = 0;
        }
        return records[index++];
    }

    private void pull() {
        Connection connection = new Connection(broker, port);
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
    }
}
