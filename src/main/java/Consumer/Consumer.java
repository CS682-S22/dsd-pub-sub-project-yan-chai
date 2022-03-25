package Consumer;

import Broker.Broker;
import Model.Connection;
import Model.DataRecord;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Author: Haoyu Yan
 * pull base consumer
 */
public class Consumer {

    private String broker;
    private int port;
    private String topic;
    private int start;
    private LinkedBlockingDeque<byte[]> bQueue;
    private String[] records;
    private int index;
    private static final Logger logger = LogManager.getLogger(Consumer.class);


    public Consumer(String b, int p, String t, int startPosition) {
        bQueue = new LinkedBlockingDeque<>();
        Thread thread = new Thread(new PullServer(b, p, t, bQueue, startPosition));
        thread.start();
    }

    public String poll(Duration duration) {
        byte[] tmp = null;
        String result = null;
        try {
            tmp = bQueue.poll(duration.toMillis(), TimeUnit.MILLISECONDS);
            if (tmp == null) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                result = new String(tmp);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return result;
    }

}
