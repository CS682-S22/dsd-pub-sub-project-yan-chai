package Producer;

import Model.Connection;
import Model.DataRecord;
import com.google.protobuf.ByteString;

import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.Socket;
import java.util.Properties;

public class Producer {

    Connection connection;


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
        connection.send(message);
    }

    public void close() {
        connection.send(DataRecord.Record.newBuilder().setTopic("finish").setMsg(ByteString.EMPTY).build().toByteArray());
        connection.close();
    }
}
