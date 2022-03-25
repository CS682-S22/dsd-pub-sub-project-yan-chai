package Broker;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;

public class DataWriter {

    private HashMap<String, ArrayList<byte[]>> map;

    public DataWriter (String[] topic) {
        map = new HashMap<>();
        for (String s :  topic) {
            map.put(s, new ArrayList<>());
        }
    }

    public synchronized void write(String topic, byte[] content) {
        ArrayList<byte[]> list = map.get(topic);
        list.add(content);
        if (list.size() >= 10) {
            BufferedOutputStream bos = null;
            try {
                bos = new BufferedOutputStream(new FileOutputStream("storage/" + topic, true));
                for (byte[] b : list) {
                    bos.write(b);
                }
                list.clear();
                bos.flush();
                bos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
