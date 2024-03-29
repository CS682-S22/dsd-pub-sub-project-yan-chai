package Broker;

import Model.Config;
import Model.Connection;
import Model.DataRecord;
import Model.FaultConnection;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Author: Haoyu Yan
 * Handle each connection
 */
public class ConnectionHandler implements Runnable{

    private int id;
    private MemberTable table;
    private FaultConnection connection;
    private Config config;
    private Storage storage;
    private ConcurrentHashMap<Integer, FaultConnection> members;
    private static final Logger logger = LogManager.getLogger(ConnectionHandler.class);

    public ConnectionHandler(int id, FaultConnection connection, Config config, MemberTable table, Storage storage) {
        this.id = id;
        this.table = table;
        this.config = config;
        this.connection = connection;
        this.storage = storage;
        this.members = table.getMembers();
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
        if (record.getTopic().equals("broker")) {
            connection.send(DataRecord.Record.newBuilder().setId(table.getLeader()).setTopic("leader").setMsg(ByteString.EMPTY).build().toByteArray());
            if (!storage.isEmpty() && id == table.getLeader()) {
                table.setBusy(true);
                String[] info = storage.getInfo().split(",");
                for (int i = 0; i < info.length; i += 2) {
                    int j = 0;
                    while (j < Integer.parseInt(info[i+1])) {
                        connection.send(DataRecord.Record.newBuilder().setId(j).setTopic(info[i]).setMsg(storage.getMsg(info[i], j)).build().toByteArray());
                        try {
                            DataRecord.Record rec = DataRecord.Record.parseFrom(connection.receive());
                            if (rec.getTopic().equals("ack")) {
                                j ++;
                            }
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
                table.setBusy(false);
            }
            members.put(record.getId(), connection);
            table.addNew(record.getId());
            Thread t = new Thread(new ReceivingHandler(record.getId(), config, table, connection, storage));
            t.start();
        } else if (record.getTopic().equals("producer")) {
            if (id == table.getLeader()) {
                connection.send(DataRecord.Record.newBuilder().setId(id).setTopic("success").setMsg(ByteString.EMPTY).build().toByteArray());
                while (true) {
                    if (table.isBusy()) {
                        connection.receive();
                        connection.send(DataRecord.Record.newBuilder().setId(id).setTopic("busy").setMsg(ByteString.EMPTY).build().toByteArray());
                    } else {
                        try {
                            DataRecord.Record data = DataRecord.Record.parseFrom(connection.receive());
                            storage.put(data.getTopic(), data.getMsg());
                            System.out.println(data);
                            for (int i = 0; i < table.getLeader(); i ++) {
                                FaultConnection c = table.getMembers().get(i);
                                if (c != null) {
                                    c.send(data.toByteArray());

                                }
                            }
                            connection.send(DataRecord.Record.newBuilder().setId(id).setTopic("ack").setMsg(ByteString.EMPTY).build().toByteArray());
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
            } else if (table.isBusy() || id == -1) {
                connection.send(DataRecord.Record.newBuilder().setId(id).setTopic("busy").setMsg(ByteString.EMPTY).build().toByteArray());
            } else if (id != table.getLeader()) {
                connection.send(DataRecord.Record.newBuilder().setId(table.getLeader()).setTopic("leader").setMsg(ByteString.EMPTY).build().toByteArray());
            }
        } else if (record.getTopic().equals("consumer")) {
            try {
                while (true) {
                    DataRecord.Record req = DataRecord.Record.parseFrom(connection.receive());
                    ByteString s = storage.getMsg(req.getTopic(), req.getId());
                    if (s != null) {
                        connection.send(DataRecord.Record.newBuilder().setId(req.getId()).setTopic(record.getTopic()).setMsg(s).build().toByteArray());
                    } else {
                        connection.send(DataRecord.Record.newBuilder().setId(req.getId()).setTopic("empty").setMsg(ByteString.EMPTY).build().toByteArray());
                    }
                }
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        }
    }
}
