package Broker;

import Model.Connection;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class Broker {

    private HashMap<String, ArrayBlockingQueue<byte[]>> map;
    private ServerSocket server;
    private boolean running;
    private static final Logger logger = LogManager.getLogger(Broker.class);
    private ArrayBlockingQueue<Connection> pushConsumer;

    public Broker(String prop) {
        Properties properties = new Properties();
        try {
            properties.load(new FileReader(prop));
        } catch (IOException e) {
            e.printStackTrace();
        }
        map = new HashMap<>();
        String[] topics = properties.getProperty("topics").split(",");
        for (String s : topics) {
            map.put(s, new ArrayBlockingQueue<>(20));
        }
        try {
            server = new ServerSocket(Integer.parseInt(properties.getProperty("port")));
        } catch (IOException e) {
            e.printStackTrace();
        }
        running = true;
        pushConsumer = new ArrayBlockingQueue<>(5);
        logger.info("Broker Server Start at port: " + Integer.parseInt(properties.getProperty("port")));
    }

    public void start() {
        while (running) {
            Socket socket = null;
            try {
                socket = server.accept();
            } catch (IOException e) {
                e.printStackTrace();
            }
            Thread t = new Thread(new Handler(socket, map, pushConsumer));
            t.start();
            logger.info("start a new thread");
        }
    }

    public void close() {
        logger.warn("broker close");
        running = false;
    }
}
