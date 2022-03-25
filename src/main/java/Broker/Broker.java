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

    private DataWriter dw;
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
        consumers = new HashMap<>();
        String[] topics = properties.getProperty("topics").split(",");
        dw = new DataWriter(topics);
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
            Thread t = new Thread(new Handler(socket, dw, consumers));
            t.start();
            logger.info("start a new thread");
        }
    }

    public void close() {
        logger.warn("broker close");
        running = false;
    }
}
