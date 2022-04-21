package Consumer;

import Broker.MessageReceiver;
import Model.Config;
import Model.Connection;
import Model.DataRecord;
import Model.FaultConnection;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.*;

public class PullServer implements Runnable{

    private int index;
    private LinkedBlockingDeque<ByteString> bQueue;
    private Config config;
    private ExecutorService executor = Executors.newSingleThreadExecutor();

    public PullServer(LinkedBlockingDeque<ByteString> bQueue, Config config) {
        this.bQueue = bQueue;
        this.config = config;
        index = 0;
    }

    @Override
    public void run() {
        Random r = new Random();
        int i = r.nextInt(config.getNumber());
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            FaultConnection c = null;
            try {
                c = new FaultConnection(config.getBrokers()[i], config.getPorts()[i]);
            } catch (IOException e) {
                i = r.nextInt(config.getNumber());
                continue;
            }
            c.send(DataRecord.Record.newBuilder().setId(-1).setTopic("consumer").setMsg(ByteString.EMPTY).build().toByteArray());
            while (true) {
                c.send(DataRecord.Record.newBuilder().setId(index).setTopic(config.getTopic()).setMsg(ByteString.EMPTY).build().toByteArray());
                try {
                    Thread.sleep(1000);
                    Future<DataRecord.Record> future = executor.submit(new MessageReceiver(c));
                    DataRecord.Record rec = future.get(5000, TimeUnit.MILLISECONDS);
                    if (rec.getTopic().equals("empty")) {
                        Thread.sleep(1000);
                        continue;
                    }
                    bQueue.add(rec.getMsg());
                    index ++;
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (TimeoutException e) {
                    break;
                }
            }
        }
    }
}
