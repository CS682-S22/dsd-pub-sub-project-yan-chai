package Broker;

import Model.Connection;
import Model.DataRecord;
import Model.FaultConnection;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectionHandler implements Runnable{

    private int id;
    private int leader;
    private FaultConnection connection;
    private Storage storage;
    private ConcurrentHashMap<Integer, FaultConnection> members;
    private static final Logger logger = LogManager.getLogger(ConnectionHandler.class);

    public ConnectionHandler(int id, FaultConnection connection, MemberTable table, Storage storage) {
        this.id = id;
        this.leader = table.getLeader();
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
            members.put(record.getId(), connection);
            connection.send(DataRecord.Record.newBuilder().setId(leader).setTopic("leader").setMsg(ByteString.EMPTY).build().toByteArray());
        } else if (record.getTopic().equals("election")) {

        }
    }
}
