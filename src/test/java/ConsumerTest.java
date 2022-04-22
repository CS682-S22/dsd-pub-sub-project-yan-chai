import Broker.*;
import Consumer.Consumer;
import Model.Config;
import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;

public class ConsumerTest {

    @Test
    public void test() {
        Config config1 = new Config("config.properties");
        Config config2 = new Config("config1.properties");
        MemberTable table = new MemberTable(config1);
        Storage storage = new Storage();
        storage.put("server", ByteString.copyFrom("test1".getBytes(StandardCharsets.UTF_8)));
        storage.put("server", ByteString.copyFrom("test2".getBytes(StandardCharsets.UTF_8)));
        storage.put("server", ByteString.copyFrom("test3".getBytes(StandardCharsets.UTF_8)));
        storage.put("aab", ByteString.copyFrom("test2".getBytes(StandardCharsets.UTF_8)));
        Broker broker = new Broker(table, config1, storage);
        Broker broker1 = new Broker(table, config2, storage);
        Thread thread1 = new Thread(broker);
        Thread thread2 = new Thread(broker1);
        thread1.start();
        thread2.start();
        Consumer consumer = new Consumer(config1);
        String line = null;
        ArrayList<String> list = new ArrayList<>();
        int i = 0;
        while ((line = consumer.poll(Duration.ofMillis(100))) != null) {
            if (i == 1) {
                thread1.interrupt();
            }
            i ++;
            list.add(line);
        }
        Assert.assertEquals(list.size(), 3);
    }
}
