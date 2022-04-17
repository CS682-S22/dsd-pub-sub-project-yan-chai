package Broker;

import Model.Connection;
import Model.DataRecord;
import Model.FaultConnection;
import Model.ServerInfo;

import java.util.*;
import java.util.concurrent.*;

public class HeartBeatServer implements Runnable{

    private ConcurrentHashMap<Integer, FaultConnection> members;
    private ConcurrentHashMap<Integer, Date> timeTable;
    private int ids;
    private int id;
    private ExecutorService executor = Executors.newSingleThreadExecutor();

    public HeartBeatServer(ConcurrentHashMap<Integer, FaultConnection> members, int ids, int id) {
        this.members = members;
        timeTable = new ConcurrentHashMap<>();
        this.ids = ids;
        this.id = id;
    }

    @Override
    public void run() {
        Timer timer = new Timer();
        Date date = new Date();
        for (int i = 0; i < ids; i ++) {
            timeTable.put(i, date);
        }
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                for (int i = 0; i < ids; i ++) {
                    FaultConnection c = members.get(i);
                    if (i == id || c == null) {
                        continue;
                    }
                    c.sendHeartBeat(id);
                    Future future = executor.submit(new HeartReceiver(c));
                    try {
                        DataRecord.Record ack = (DataRecord.Record) future.get(100, TimeUnit.MILLISECONDS);
                        timeTable.put(ack.getId(), new Date());
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    } catch (TimeoutException e) {
                        System.out.println("broker " + i + " lose heart beat.");
                        if (new Date().getTime() - timeTable.get(i).getTime() > 5000) {
                            members.remove(i);
                            System.out.println("remove " + i);
                        }
                    }
                }
            }
        };
        timer.schedule(task, 0, 1000);


    }
}
