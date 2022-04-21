package Broker;

import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Storage {

    private ConcurrentHashMap<String, List> map;
    private int version;

    public Storage() {
        version = 0;
        map = new ConcurrentHashMap<>();
    }

    public void put(String topic, ByteString msg) {
        List<ByteString> tmp = map.computeIfAbsent(topic, k -> Collections.synchronizedList(new ArrayList<>()));
        version ++;
        tmp.add(msg);
    }

    public ByteString getMsg(String topic, int id) {
        List<ByteString> tmp = map.get(topic);
        if (tmp.size() <= id) {
            return null;
        }
        return tmp.get(id);
    }

    public int getVersion() {
        return version;
    }
}
