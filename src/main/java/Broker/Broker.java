package Broker;

import Model.Config;
import Model.Connection;
import Model.DataRecord;
import Model.FaultConnection;
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


    private MemberTable table;
    private Config config;
    private boolean running;
    private ServerSocket server;
    private Storage storage;
    private static final Logger logger = LogManager.getLogger(Broker.class);

    public Broker(MemberTable table, Config config, Storage storage) {
        this.table = table;
        this.config = config;
        this.storage = storage;
        running = true;
        try {
            server = new ServerSocket(config.getPort());
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Broker Server Start at port: " + config.getPort());
    }

    @Override
    public void run() {
        System.out.println("Heart Beat Start");
        while (true) {
            try {
                FaultConnection c = new FaultConnection(server.accept());
                if (!running) {
                    break;
                }
                System.out.println("new Connection");
                Thread handler = new Thread(new ConnectionHandler(config.getId(), c, table, storage));
                handler.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void fail() {
        int ids = config.getNumber();
        int id = config.getId();
        ConcurrentHashMap<Integer, FaultConnection> members = table.getMembers();
        for (int i = 0; i < ids; i ++) {
            if (i != id && members.get(i) != null) {
                members.get(i).fail();
            }
        }
        logger.info("Connection fail!!!");
    }

}
