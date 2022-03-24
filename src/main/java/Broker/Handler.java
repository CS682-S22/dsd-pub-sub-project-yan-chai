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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class Handler implements Runnable {

    private Connection connection;
    private HashMap<String, ArrayBlockingQueue<byte[]>> map;
    private HashMap<String, ArrayBlockingQueue<Connection>> pushConsumer;
    private static final Logger logger = LogManager.getLogger(Broker.class);

    public Handler(Socket socket, HashMap<String, ArrayBlockingQueue<byte[]>> data, HashMap<String, ArrayBlockingQueue<Connection>> push) {
        map = data;
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
                ArrayBlockingQueue<byte[]> queue = map.get(record.getTopic());
                queue.add(tmp);
                if (queue.size() >= 10) {
                    BufferedOutputStream bos = null;
                    try {
                        bos = new BufferedOutputStream(new FileOutputStream("Storage/" + record.getTopic(), true));
                        for (byte[] b : queue) {
                            bos.write(b);
                        }
                        queue.clear();
                        bos.flush();
                        bos.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
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
                fis = new FileInputStream("Storage/" + record.getTopic());
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            int offset = Integer.parseInt(new String(record.getMsg().toByteArray()));
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
