import Model.DataRecord;
import Producer.Producer;
import com.google.protobuf.ByteString;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class TestProducter {

    @Test
    public void test() {
        Producer producer = new Producer("config.properties");
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            producer.send(DataRecord.Record.newBuilder().setMsg(ByteString.copyFrom("hello! This is a Test!", StandardCharsets.UTF_8)).setTopic("pTest").build().toByteArray());
        }
    }
}
