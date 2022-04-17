package Broker;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

public class DataWriter {

    private BufferedOutputStream bos;

    public DataWriter (String topic) {
        try {
            bos = new BufferedOutputStream(new FileOutputStream(topic));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public synchronized void write(ArrayList<byte[]> list) {
        for (byte[] b : list) {
            try {
                bos.write(b);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
