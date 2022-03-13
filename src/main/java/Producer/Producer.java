package Producer;

import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.Socket;
import java.util.Properties;

public class Producer {

    Socket socket;


    public Producer(String prop) {
        Properties properties = new Properties();
        try {
            properties.load(new FileReader(prop));
        } catch (IOException e) {
            e.printStackTrace();
        }
        String broker = properties.getProperty("broker");
        int port = Integer.parseInt(properties.getProperty("port"));
        try {
            socket = new Socket(broker, port);
        } catch (IOException e) {
            System.out.println("Create Socket Error!");
        }
        if (socket == null) {
            System.exit(-1);
        }
    }

    public void send(byte[] message) {
        try {
            DataOutputStream dOut = new DataOutputStream(socket.getOutputStream());
            dOut.writeInt(message.length);
            dOut.write(message);
            dOut.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
