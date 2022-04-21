package Model;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class Config {
    private String loc;
    private int port;
    private int id;
    private String[] brokers;
    private int[] ports;
    private int number;
    private String input;
    private String topic;

    public Config(String prop) {
        Properties properties = new Properties();
        try {
            properties.load(new FileReader(prop));
        } catch (IOException e) {
            e.printStackTrace();
        }
        id = Integer.parseInt(properties.getProperty("id"));
        loc = properties.getProperty("broker");
        port = Integer.parseInt(properties.getProperty("port"));
        input = properties.getProperty("input");
        topic = properties.getProperty("topic");
        brokers = properties.getProperty("brokers").split(",");
        String[] tmp = properties.getProperty("ports").split(",");
        number = tmp.length;
        ports  = new int[number];
        for (int i = 0; i < number; i++) {
            ports[i] = Integer.parseInt(tmp[i]);
        }
    }

    public String getLoc() {
        return loc;
    }

    public int getPort() {
        return port;
    }

    public int getId() {
        return id;
    }

    public String[] getBrokers() {
        return brokers;
    }

    public int[] getPorts() {
        return ports;
    }

    public int getNumber() {
        return number;
    }

    public String getInput() {
        return input;
    }

    public String getTopic() {
        return topic;
    }
}
