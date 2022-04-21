import Broker.*;
import Consumer.Consumer;
import Model.Config;
import com.google.protobuf.ByteString;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;

public class ConsumerTest {

    @Test
    public void test() {
        Config config = new Config("config.properties");
        MemberTable table = new MemberTable(config);
        Storage storage = new Storage();
        storage.put("server", ByteString.copyFrom("test1".getBytes(StandardCharsets.UTF_8)));
        storage.put("server", ByteString.copyFrom("test2".getBytes(StandardCharsets.UTF_8)));
        storage.put("server", ByteString.copyFrom("test3".getBytes(StandardCharsets.UTF_8)));
        storage.put("aab", ByteString.copyFrom("test2".getBytes(StandardCharsets.UTF_8)));
        Broker broker = new Broker(table, config, storage);
        Thread thread = new Thread(broker);
        thread.start();
        Consumer consumer = new Consumer(config);
        String line = null;
        ArrayList<String> list = new ArrayList<>();
        while ((line = consumer.poll(Duration.ofMillis(100))) != null) {
            list.add(line);
        }
        System.out.println(list);
    }
}
