package Model;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.Socket;

public class FaultConnection extends Connection {

    private boolean running;

    public FaultConnection(String host, int port) throws IOException {
        super(host, port);
        running = true;
    }

    public FaultConnection(Socket socket) {
        super(socket);
        running = true;
    }

    @Override
    public byte[] receive() {
        if (running) {
            return super.receive();
        }
        return null;
    }

    @Override
    public boolean send(byte[] message) {
        if (running) {
            return super.send(message);
        }
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    public void fail() {
        running = false;
    }

    public void sendHeartBeat(int id) {
        send(DataRecord.Record.newBuilder().setId(id).setTopic("heart").setMsg(ByteString.EMPTY).build().toByteArray());
    }
}
