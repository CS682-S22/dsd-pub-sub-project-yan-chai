package Driver;

import Consumer.Consumer;

public class ConsumerDriver {

    public static void main(String[] args) {
        Consumer consumer = new Consumer("127.0.0.1", 30000, "music", 0);
        
    }
}
