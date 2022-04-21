package Producer;

import Broker.Broker;
import Model.Config;
import Model.Connection;
import Model.DataRecord;
import Model.FaultConnection;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import Broker.MessageReceiver;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.*;

/**
 * Author: Haoyu Yan
 * Producer, send data to broker
 */
public class Producer implements Runnable{

    private Config config;
    private int index;
    private BufferedReader br;
    private String line;
    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private static final Logger logger = LogManager.getLogger(Producer.class);


    public Producer(Config config) {
        this.config = config;
        try {
            System.out.println(config.getInput());
            br = new BufferedReader(new FileReader(config.getInput()));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        index = 0;
    }


    @Override
    public void run() {
        FaultConnection connection = null;
        Random r = new Random();
        int i = r.nextInt(config.getNumber());
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Connect to: " + i);
            try {
                connection = new FaultConnection(config.getBrokers()[i], config.getPorts()[i]);
            } catch (IOException e) {
                i = r.nextInt(config.getNumber());
                continue;
            }
            connection.send(DataRecord.Record.newBuilder().setId(-1).setTopic("producer").setMsg(ByteString.EMPTY).build().toByteArray());
            Future<DataRecord.Record> future = executor.submit(new MessageReceiver(connection));
            DataRecord.Record rec = null;
            try {
                rec = future.get(1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                continue;
            }
            if (rec.getTopic().equals("leader")) {
                i = rec.getId();
            } else if (rec.getTopic().equals("success")) {
                boolean send = true;
                while (true) {
                    try {
                        if (send) {
                            line = br.readLine();
                            send = false;
                        }
                        if (line == null) {
                            break;
                        }
                        Thread.sleep(1000);
                        connection.send(DataRecord.Record.newBuilder().setId(index++).setTopic(config.getTopic()).setMsg(ByteString.copyFrom(line.getBytes(StandardCharsets.UTF_8))).build().toByteArray());
                        future = executor.submit(new MessageReceiver(connection));
                        rec = future.get(5000, TimeUnit.MILLISECONDS);
                        if (rec == null) {
                            break;
                        }
                        if (rec.getTopic().equals("ack")) {
                            send = true;
                            continue;
                        } else if (rec.getTopic().equals("busy")) {
                            Thread.sleep(5000);
                            send = false;
                            continue;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (TimeoutException e) {
                        break;
                    }
                }
            } else {
                i = r.nextInt(config.getNumber());
            }
        }
    }
}
