package Consumer;

import Broker.MessageReceiver;
import Model.Config;
import Model.Connection;
import Model.DataRecord;
import Model.FaultConnection;
import com.google.protobuf.ByteString;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.*;

/**
 * Author: Haoyu Yan
 * Conduer class
 */
public class Consumer {

    private Config config;
    private int index;
    private LinkedBlockingDeque<ByteString> list;
    private PullServer server;
    private ExecutorService executor = Executors.newSingleThreadExecutor();

    public Consumer(Config config) {
        this.config = config;
        index = 0;
        list = new LinkedBlockingDeque<>();
        server = new PullServer(list, config);
        Thread thread = new Thread(server);
        thread.start();
    }

    public String poll(Duration duration) {
        ByteString tmp;
        String result = null;
        try {
            tmp = list.poll(duration.toMillis(), TimeUnit.MILLISECONDS);
            if (tmp == null) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                result = tmp.toString(StandardCharsets.UTF_8);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return result;
    }

}
