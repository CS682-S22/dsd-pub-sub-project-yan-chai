import Consumer.Consumer;
import org.junit.Test;

import java.time.Duration;

public class TestConsumer {

    @Test
    public void test() {
        StringBuffer buffer = new StringBuffer();
        Consumer consumer = new Consumer("127.0.0.1", 30000, "test", 0);
        while (true) {
            System.out.println(consumer.poll(Duration.ofMillis(100)));;
        }
    }
}
