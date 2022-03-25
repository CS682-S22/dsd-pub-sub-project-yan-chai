package Driver;

import Consumer.Consumer;

import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

/**
 * Author: Haoyu Yan
 * Consumer Driver
 */
public class ConsumerDriver {

    public static void main(String[] args) {

        Properties prop = new Properties();
        try {
            prop.load(new FileReader("config.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        Consumer consumer = new Consumer(prop.getProperty("broker"), Integer.parseInt(prop.getProperty("port")), prop.getProperty("topic"), 0);

        while (true) {
            String tmp = consumer.poll(Duration.ofMillis(100));
            System.out.println(tmp);
        }
    }
}
