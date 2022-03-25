package Driver;

import Model.DataRecord;
import Producer.Producer;
import com.google.protobuf.ByteString;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * Author: Haoyu Yan
 * Producer Driver
 */
public class ProducerDriver {

    public static void main(String[] args) {
        Properties prop = new Properties();
        try {
            prop.load(new FileReader("config.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        Producer producer = new Producer("config.properties");
        String topic = prop.getProperty("topic");
        String infile = prop.getProperty("input");
        int bytes = 1024;
        try (FileInputStream fis = new FileInputStream(infile)) {
            byte[] tmp = new byte[bytes];
            int len;
            while ((len = fis.read(tmp, 0, bytes)) != -1) {
                byte[] s;
                s = Arrays.copyOf(tmp, len);
                producer.send(DataRecord.Record.newBuilder().setTopic(topic).setMsg(ByteString.copyFrom(s)).build().toByteArray());
                Thread.sleep(500);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        producer.close();
    }
}
