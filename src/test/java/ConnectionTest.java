import Broker.MessageReceiver;
import Model.DataRecord;
import Model.FaultConnection;
import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.*;

public class ConnectionTest {

    @Test
    public void test(){
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Thread t = new Thread(() -> {
            try {
                ServerSocket ss = new ServerSocket(30000);
                FaultConnection c = new FaultConnection(ss.accept());
                while (true) {
                    c.receive();
                    c.fail();
                    c.send(DataRecord.Record.newBuilder().setId(0).setTopic("topic").setMsg(ByteString.EMPTY).build().toByteArray());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        t.start();
        try {
            FaultConnection c = new FaultConnection(new Socket("127.0.0.1", 30000));
            c.send(DataRecord.Record.newBuilder().setId(0).setTopic("topic").setMsg(ByteString.EMPTY).build().toByteArray());
            Future future = executor.submit(new MessageReceiver(c));
            DataRecord.Record dr = (DataRecord.Record) future.get(100, TimeUnit.MILLISECONDS);
            Assert.fail();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            System.out.println("Time out");
        }
    }
}
