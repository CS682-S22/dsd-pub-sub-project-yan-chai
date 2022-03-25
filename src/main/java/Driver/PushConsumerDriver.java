package Driver;

import Consumer.PushConsumer;

/**
 * Author: Haoyu Yan
 * Push Base Consumer Driver
 */
public class PushConsumerDriver {
    public static void main(String[] args) {

        PushConsumer pc = new PushConsumer("127.0.0.1", 30000, "music");

        pc.start();
    }
}
