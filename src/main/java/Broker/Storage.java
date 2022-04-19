package Broker;

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
        List<String> a = Collections.synchronizedList(new ArrayList<>());
    }

    public void put(String topic, String msg) {
        List<String> tmp = map.computeIfAbsent(topic, k -> Collections.synchronizedList(new ArrayList<>()));
        version ++;
        tmp.add(msg);
    }

    public String getMsg(String topic, int id) {
        List<String> tmp = map.get(topic);
        return tmp.get(id);
    }

    public int getVersion() {
        return version;
    }
}
