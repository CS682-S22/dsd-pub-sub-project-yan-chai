package Broker;

import Model.Connection;
import Model.DataRecord;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Author: Haoyu Yan
 * Handler, handle thread from broker
 */
public class Handler implements Runnable {

    private Connection connection;
    private DataWriter dataWriter;
    private HashMap<String, ArrayBlockingQueue<Connection>> pushConsumer;
    private static final Logger logger = LogManager.getLogger(Broker.class);

    public Handler(Socket socket, DataWriter dw, HashMap<String, ArrayBlockingQueue<Connection>> push) {
        dataWriter = dw;
        connection = new Connection(socket);
        pushConsumer = push;
    }

    @Override
    public void run() {
        byte[] tmp = connection.receive();
        DataRecord.Record record = null;
        try {
            record = DataRecord.Record.parseFrom(tmp);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        if (record.getTopic().equals("producer")) {
            logger.info("producer thread");
            while (true) {
                try {
                    tmp = connection.receive();
                    if (tmp == null) continue;
                    record = DataRecord.Record.parseFrom(tmp);
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
                if (record.getTopic().equals("finish")) break;
                tmp = record.getMsg().toByteArray();
                dataWriter.write(record.getTopic(), tmp);
            }
        } else if (record.getTopic().equals("consumer")) {
            logger.info("consumer thread");
            try {
                record = DataRecord.Record.parseFrom(connection.receive());
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
            FileInputStream fis = null;
            try {
                fis = new FileInputStream("storage/" + record.getTopic());
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            int offset = Integer.parseInt(new String(record.getMsg().toByteArray()));
            logger.info("send to consumer, offset: "+ offset);
            try {
                fis.getChannel().position(offset);
            } catch (IOException e) {
                e.printStackTrace();
            }
            tmp = new byte[1024];
            try {
                int len = fis.read(tmp, 0, 1024);
                if (len <= 0) {
                    connection.send(DataRecord.Record.newBuilder().setMsg(ByteString.EMPTY).setTopic("finish").build().toByteArray());
                } else {
                    byte[] s;
                    s = Arrays.copyOf(tmp, len);
                    connection.send(DataRecord.Record.newBuilder().setId(offset+len).setMsg(ByteString.copyFrom(s)).setTopic(record.getTopic()).build().toByteArray());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (record.getTopic().equals("push")) {
            String t = new String(record.getMsg().toByteArray());
            ArrayBlockingQueue<Connection> list = pushConsumer.get(t);
            if (list == null) {
                list = new ArrayBlockingQueue<>(5);
                pushConsumer.put(t, list);
            }
            list.add(connection);
        }
        else {
            System.out.println("Initial Error!");
        }
    }
}
