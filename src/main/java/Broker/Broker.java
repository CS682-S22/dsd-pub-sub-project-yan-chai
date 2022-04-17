package Broker;

import Model.Connection;
import Model.DataRecord;
import Model.FaultConnection;
import Model.ServerInfo;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class Broker implements Runnable{


    private int id;
    private int leader;
    private ConcurrentHashMap<String, DataWriter> writers;
    private ServerSocket server;
    private boolean running;
    private static final Logger logger = LogManager.getLogger(Broker.class);
    private ConcurrentHashMap<Integer, FaultConnection> members;
    private int ids;
    private HeartBeatServer hbs;

    public Broker(String prop) {
        Properties properties = new Properties();
        try {
            properties.load(new FileReader(prop));
        } catch (IOException e) {
            e.printStackTrace();
        }
        id = Integer.parseInt(properties.getProperty("id"));
        writers = new ConcurrentHashMap<String, DataWriter>();
        try {
            server = new ServerSocket(Integer.parseInt(properties.getProperty("port")));
        } catch (IOException e) {
            e.printStackTrace();
        }
        running = true;
        String[] brokers = properties.getProperty("brokers").split(",");
        String[] tmp = properties.getProperty("ports").split(",");
        ids = tmp.length;
        int[] ports  = new int[ids];
        for (int i = 0; i < ids; i++) {
            ports[i] = Integer.parseInt(tmp[i]);
        }
        numberTableInit(brokers, ports);
        hbs = new HeartBeatServer(members, ids, id);
        logger.info("Broker Server Start at port: " + Integer.parseInt(properties.getProperty("port")));
    }

    private void numberTableInit(String[] brokers, int[] ports) {
        members = new ConcurrentHashMap<>();
        FaultConnection c = null;
        for (int i = 0; i < brokers.length; i ++) {
            if (i == id) {
                continue;
            }
            try {
                c = new FaultConnection(brokers[i], ports[i]);
            } catch (IOException ignored) { }
            if (c == null) {
                logger.info("Cannot connect to " + brokers[i]);
                continue;
            }
            c.send(DataRecord.Record.newBuilder().setId(id).setTopic("broker").setMsg(ByteString.EMPTY).build().toByteArray());
            try {
                DataRecord.Record record = DataRecord.Record.parseFrom(c.receive());
                leader = record.getId();
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
            members.put(i, c);
        }
        if (members.size() == 0) {
            leader = id;
        }
    }

    @Override
    public void run() {
        Thread t = new Thread(hbs);
        t.start();
        System.out.println("Heart Beat Start");
        while (running) {
            try {
                FaultConnection c = new FaultConnection(server.accept());
                System.out.println("new Connection");
                Thread handler = new Thread(new ConnectionHandler(id, leader, c, writers, members));
                handler.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public ConcurrentHashMap<Integer, FaultConnection> getMembers() {
        return members;
    }

    public void fail() {
        for (int i = 0; i < ids; i ++) {
            if (i != id && members.get(i) != null) {
                members.get(i).fail();
            }
        }
        logger.info("Connection fail!!!");
    }

    public int getLeader() {
        return leader;
    }
}
