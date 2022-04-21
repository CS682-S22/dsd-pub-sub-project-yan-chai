package Broker;

import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Storage {

    private ConcurrentHashMap<String, List> map;

    public Storage() {
        map = new ConcurrentHashMap<>();
    }

    public void put(String topic, ByteString msg) {
        List<ByteString> tmp = map.computeIfAbsent(topic, k -> Collections.synchronizedList(new ArrayList<>()));
        tmp.add(msg);
    }

    public ByteString getMsg(String topic, int id) {
        List<ByteString> tmp = map.get(topic);
        if (tmp.size() <= id) {
            return null;
        }
        return tmp.get(id);
    }

    public String getInfo() {
        StringBuffer res = new StringBuffer();
        int index = 0;
        for (Map.Entry<String, List> e : map.entrySet()) {
            res.append(e.getKey());
            res.append(",");
            res.append(e.getValue().size());
            res.append(",");
        }
        return res.deleteCharAt(res.length()-1).toString();
    }

    public int getVersion(String topic) {
        return map.get(topic).size();
    }

    public void rollback(String topic) {
        int index = map.get(topic).size() - 1;
        map.get(topic).remove(index);
    }
}
