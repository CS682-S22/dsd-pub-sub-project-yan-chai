package Broker;

import Model.Connection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Author: Haoyu Yan
 * Broker, support both poll and push base consumer
 */
public class Broker {

    private HashMap<String, ArrayBlockingQueue<byte[]>> map;
    private ServerSocket server;
    private boolean running;
    private static final Logger logger = LogManager.getLogger(Broker.class);
    private HashMap<String, ArrayBlockingQueue<Connection>> consumers;

    public Broker(String prop) {
        Properties properties = new Properties();
        try {
            properties.load(new FileReader(prop));
        } catch (IOException e) {
            e.printStackTrace();
        }
        map = new HashMap<>();
        consumers = new HashMap<>();
        String[] topics = properties.getProperty("topics").split(",");
        for (String s : topics) {
            map.put(s, new ArrayBlockingQueue<>(20));
            consumers.put(s, new ArrayBlockingQueue<Connection>(5));
        }
        try {
            server = new ServerSocket(Integer.parseInt(properties.getProperty("port")));
        } catch (IOException e) {
            e.printStackTrace();
        }
        running = true;
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
            Thread t = new Thread(new Handler(socket, map, consumers));
            t.start();
            logger.info("start a new thread");
        }
    }

    public void close() {
        logger.warn("broker close");
        running = false;
    }
}
