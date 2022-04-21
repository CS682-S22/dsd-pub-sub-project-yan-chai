package Broker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Sync {

    private ConcurrentHashMap<String, Integer> sync;
    private int syncCount;

    public Sync () {
        sync = new ConcurrentHashMap<>();
        syncCount = 0;
    }

    public void newSync() {
        syncCount ++;
    }

    public void sync(String content) {
        String[] tmp = content.split(",");
        if (syncCount == 0) {
            for (int i = 0; i < tmp.length; i+=2) {
                sync.put(tmp[i], Integer.parseInt(tmp[i+1]));
            }
        } else {
            for (int i = 0; i < tmp.length; i+=2) {
                if (sync.get(tmp[i]) > Integer.parseInt(tmp[i+1])) {
                    sync.put(tmp[i], Integer.parseInt(tmp[i+1]));
                }
            }
        }
    }

    public int getSyncCount() {
        return syncCount;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (Map.Entry<String, Integer> e : sync.entrySet()) {
            sb.append(e.getKey());
            sb.append(",");
            sb.append(e.getValue());
            sb.append(",");
        }
        return sb.deleteCharAt(sb.length()-1).toString();
    }
}
