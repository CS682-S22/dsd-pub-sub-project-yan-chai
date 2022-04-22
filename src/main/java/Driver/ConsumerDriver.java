package Driver;

import Consumer.Consumer;
import Model.Config;

import java.time.Duration;

/**
 * Author: Haoyu Yan
 * Consumer Driver Class, run Consumer
 */
public class ConsumerDriver {

    public static void main(String[] args) {
        Config config = new Config("config.properties");
        Consumer consumer = new Consumer(config);
        while (true) {
            System.out.println(consumer.poll(Duration.ofMillis(1000)));
        }
    }
}
