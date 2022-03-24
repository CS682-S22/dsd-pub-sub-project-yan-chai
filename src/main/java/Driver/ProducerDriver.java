package Driver;

import Model.DataRecord;
import Producer.Producer;
import com.google.protobuf.ByteString;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

public class ProducerDriver {

    public static void main(String[] args) {
        Producer producer = new Producer("config.properties");
        String topic = "music";
        String infile = "input";
        int bytes = 1024;
        try (FileInputStream fis = new FileInputStream(infile)) {
            byte[] tmp = new byte[bytes];
            int len;
            while ((len = fis.read(tmp, 0, bytes)) != -1) {
                byte[] s;
                s = Arrays.copyOf(tmp, len);
                producer.send(DataRecord.Record.newBuilder().setTopic(topic).setMsg(ByteString.copyFrom(s)).build().toByteArray());
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
