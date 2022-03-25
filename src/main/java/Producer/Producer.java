package Producer;

import Broker.Broker;
import Model.Connection;
import Model.DataRecord;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.Socket;
import java.util.Properties;

/**
 * Author: Haoyu Yan
 * Producer, send data to broker
 */
public class Producer {

    Connection connection;
    private static final Logger logger = LogManager.getLogger(Broker.class);


    public Producer(String prop) {
        Properties properties = new Properties();
        try {
            properties.load(new FileReader(prop));
        } catch (IOException e) {
            e.printStackTrace();
        }
        String broker = properties.getProperty("broker");
        int port = Integer.parseInt(properties.getProperty("port"));
        connection = new Connection(broker, port);
        connection.send(DataRecord.Record.newBuilder().setTopic("producer").setMsg(ByteString.EMPTY).build().toByteArray());
    }

    public void send(byte[] message) {
        logger.info("send new message");
        connection.send(message);
    }

    public void close() {
        connection.send(DataRecord.Record.newBuilder().setTopic("finish").setMsg(ByteString.EMPTY).build().toByteArray());
        connection.close();
    }
}
