package Driver;

import Consumer.PushConsumer;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Author: Haoyu Yan
 * Push Base Consumer Driver
 */
public class PushConsumerDriver {
    public static void main(String[] args) {
        Properties prop = new Properties();
        try {
            prop.load(new FileReader("config.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        PushConsumer pc = new PushConsumer(prop.getProperty("broker"), Integer.parseInt(prop.getProperty("port")), prop.getProperty("topic"));

        pc.start();
    }
}
