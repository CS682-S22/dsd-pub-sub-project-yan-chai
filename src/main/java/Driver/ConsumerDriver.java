package Driver;

import Consumer.Consumer;

import java.time.Duration;

/**
 * Author: Haoyu Yan
 * Consumer Driver
 */
public class ConsumerDriver {

    public static void main(String[] args) {
        Consumer consumer = new Consumer("127.0.0.1", 30000, "music", 0);

        while (true) {
            String tmp = consumer.poll(Duration.ofMillis(100));
            System.out.println(tmp);
        }
    }
}
