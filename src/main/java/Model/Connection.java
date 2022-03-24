package Model;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * Author: Haoyu Yan
 * Simply, do not lose connection
 */
public class Connection{

    Socket socket;

    /**
     * Connection Constructor
     * @param host
     * @param port
     */
    public Connection(String host, int port) {
        try {
            socket = new Socket(host, port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Connection(Socket socket) {
        this.socket = socket;
    }

    public byte[] receive() {
        byte[] message = null;
        try {
            DataInputStream dIn = new DataInputStream(socket.getInputStream());
            int length = dIn.readInt();
            if(length>0) {
                message = new byte[length];
                dIn.readFully(message, 0, message.length);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return message;
    }

    public boolean send(byte[] message) {
        try {
            DataOutputStream dOut = new DataOutputStream(socket.getOutputStream());
            dOut.writeInt(message.length);
            dOut.write(message);
            dOut.flush();
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    public void close() {
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
